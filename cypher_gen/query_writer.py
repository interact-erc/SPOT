from graph import Graph
from utils import CYPHER_DATATYPES


class CypherQueryWriter:

    def __init__(self, graph: Graph):
        self.graph = graph
        self.nl_provider = None
        self.cast_modifiers = False
        self.use_target_subreference = True
        self.use_domain_for_nl_rel_direction = False

    def set_nl_provider(self, provider):
        self.nl_provider = provider

    def _get_select_variable_line(self):
        answer_node = self.graph.answer_node
        if answer_node.modifier:
            if answer_node.modifier == "count":
                if self.graph.return_attribute:
                    line = f"RETURN COUNT(DISTINCT x{answer_node.id}.{self.graph.return_property})"
                else:
                    line = f"RETURN COUNT(DISTINCT x{answer_node.id})"
            elif answer_node.modifier == "avg":
                line = f"RETURN AVG(x{answer_node.id}.{answer_node.attribute})"
                if self.cast_modifiers:
                    line = f"RETURN AVG({CYPHER_DATATYPES[answer_node.datatype]}(x{answer_node.id}.{answer_node.attribute}))"
            elif answer_node.modifier == "sum":
                line = f"RETURN SUM(x{answer_node.id}.{answer_node.attribute})"
                if self.cast_modifiers:
                    line = f"RETURN SUM({CYPHER_DATATYPES[answer_node.datatype]}(x{answer_node.id}.{answer_node.attribute}))"
            elif answer_node.modifier == "argmin" or answer_node.modifier == "argmax":
                line = f"RETURN x{answer_node.id}"
            else:  # max min
                if not self.graph.return_attribute:
                    line = f"RETURN x{answer_node.id}"
                else:
                    line = f"RETURN x{answer_node.id}.{self.graph.return_property}"
        else:
            if self.graph.return_attribute:
                line = f"RETURN x{answer_node.id}.{self.graph.return_property}"
            else:
                line = f"RETURN x{answer_node.id}"

        return [line]

    def _get_entity_ground_clause(self, node):
        return f'WHERE x{node.id}.{node.attribute} = "{node.grounded_entity}"'

    def _get_triplet_chain_lines_recursive(self, chain_line, node, relation):
        query_lines = []
        obj = f"x{node.id}"
        obj_type = f"{node.class_type}"
        where_clause = ""
        if node.grounded_entity:
            if not node.modifier:
                e_c = self._get_entity_ground_clause(node)
                where_clause = " " + e_c
        node_clause = f"({obj}:{obj_type}{where_clause})"

        if len(node.in_relations + node.out_relations) == 1:  # Edge node
            return [chain_line + node_clause]
        expanded_chain_line_once = (
            False  # If we expand a second time we don't repeat the chain
        )
        for rel in node.in_relations:
            if rel != relation:
                if not expanded_chain_line_once:
                    new_chain = f"{chain_line}{node_clause}-[:{rel.rel_type}]-"
                    expanded_chain_line_once = True
                else:
                    new_chain = f"MATCH {node_clause}-[:{rel.rel_type}]-"
                q_l = self._get_triplet_chain_lines_recursive(
                    new_chain, rel.in_node, rel
                )
                query_lines += q_l
        for rel in node.out_relations:
            if rel != relation:
                if not expanded_chain_line_once:
                    new_chain = f"{chain_line}{node_clause}-[:{rel.rel_type}]-"
                    expanded_chain_line_once = True
                else:
                    new_chain = f"MATCH {node_clause}-[:{rel.rel_type}]-"
                q_l = self._get_triplet_chain_lines_recursive(
                    new_chain, rel.out_node, rel
                )
                query_lines += q_l

        return query_lines

    def _get_triplet_chain_lines(self):
        node = self.graph.answer_node
        query_lines = []
        obj = f"x{node.id}"
        obj_type = f"{node.class_type}"
        where_clause = ""
        if node.grounded_entity:
            # if not node.modifier:
            e_c = self._get_entity_ground_clause(node)
            where_clause = " " + e_c
        first_match = f"MATCH ({obj}:{obj_type}{where_clause})"
        for relation in node.in_relations:
            chain_line = first_match + f"-[:{relation.rel_type}]-"
            q_l = self._get_triplet_chain_lines_recursive(
                chain_line, relation.in_node, relation
            )
            query_lines += q_l
        for relation in node.out_relations:
            chain_line = first_match + f"-[:{relation.rel_type}]-"
            q_l = self._get_triplet_chain_lines_recursive(
                chain_line, relation.out_node, relation
            )
            query_lines += q_l

        return query_lines

    def _get_modifier_with_line(self, node):
        lines = []
        if node.modifier.startswith("count") and node.modifier != "count":
            line1 = f"WITH x{self.graph.answer_node.id}, COUNT(x{node.id}.{node.attribute}) as z{node.id}"
            line2 = f"WHERE z{node.id} {node.modifier[len('count'):]} {node.grounded_entity}"
            lines += [line1, line2]
        elif node.modifier.startswith("sum") and node.modifier != "sum":
            if self.cast_modifiers:
                line1 = f"WITH x{self.graph.answer_node.id}, SUM({CYPHER_DATATYPES[node.datatype]}(x{node.id}.{node.attribute})) as z{node.id}"
            else:
                line1 = f"WITH x{self.graph.answer_node.id}, SUM(x{node.id}.{node.attribute}) as z{node.id}"
            line2 = (
                f"WHERE z{node.id} {node.modifier[len('sum'):]} {node.grounded_entity}"
            )
            lines += [line1, line2]
        elif node.modifier.startswith("avg") and node.modifier != "avg":
            if self.cast_modifiers:
                line1 = f"WITH x{self.graph.answer_node.id}, AVG({CYPHER_DATATYPES[node.datatype]}(x{node.id}.{node.attribute})) as z{node.id}"
            else:
                line1 = f"WITH x{self.graph.answer_node.id}, AVG(x{node.id}.{node.attribute}) as z{node.id}"
            line2 = (
                f"WHERE z{node.id} {node.modifier[len('avg'):]} {node.grounded_entity}"
            )
            lines += [line1, line2]
        elif node.modifier in ("<", ">", "=", "<=", ">="):
            if self.cast_modifiers:
                line1 = f"WHERE {CYPHER_DATATYPES[node.datatype]}(x{node.id}.{node.attribute}) {node.modifier} {node.grounded_entity}"
            else:
                line1 = f'WHERE x{node.id}.{node.attribute} {node.modifier} "{node.grounded_entity}"'
            lines += [line1]

        return lines

    def _get_modifier_closing_line(self, node):
        lines = []
        if node.modifier in ("min", "max", "argmin", "argmax"):
            if self.cast_modifiers:
                line = f"ORDER BY {CYPHER_DATATYPES[node.datatype]}(x{node.id}.{node.attribute})"
            else:
                line = f"ORDER BY x{node.id}.{node.attribute}"
            if node.modifier == "max" or node.modifier == "argmax":
                line += " DESC"
            else:  # min
                line += " ASC"
            lines.append(line)
            lines.append("LIMIT 1")

        return lines

    def write_query(self):
        query_lines = []

        q_l = self._get_triplet_chain_lines()
        query_lines += q_l

        for node in self.graph.nodes:
            if node.modifier:
                q_l = self._get_modifier_with_line(node)
                query_lines += q_l

        q_l = self._get_select_variable_line()
        query_lines += q_l

        for node in self.graph.nodes:
            if node.modifier:
                q_l = self._get_modifier_closing_line(node)
                query_lines += q_l
        return "\n".join(query_lines)

    def _extract_recursive_proto_nls(self, proto):
        nl_string = proto.get("nl", "")

        substrings = []
        for out in proto.get("out_nls", []):
            out_strings = self._extract_recursive_proto_nls(out)
            substrings += out_strings
        for inn in proto.get("in_nls", []):
            in_strings = self._extract_recursive_proto_nls(inn)
            substrings += in_strings

        strings = []
        if substrings:
            for i in substrings:
                strings.append(nl_string + " " + i)
        else:
            strings.append(nl_string)

        return strings

    def _get_sub_reference_main_line_proto_nl(self):
        return f"the target {self.nl_provider.get_class_type_nl(self.graph.answer_node.class_type)}"

    def _proto_nl_to_str(self, proto):
        main_line = proto["nl"]

        substrings = []
        for out in proto.get("out_nls", []):
            out_strings = self._extract_recursive_proto_nls(out)
            substrings += out_strings
        for inn in proto.get("in_nls", []):
            in_strings = self._extract_recursive_proto_nls(inn)
            substrings += in_strings

        if self.use_target_subreference:
            sub_main_line = self._get_sub_reference_main_line_proto_nl()
            substr_1 = substrings[:1]
            substr_2 = substrings[1:]
            substr_1 = [main_line + " " + i for i in substr_1]
            substr_2 = [sub_main_line + " " + i for i in substr_2]
            substrings = substr_1 + substr_2
        else:
            substrings = [main_line + " " + i for i in substrings]
        substrings = "\nAND ".join(substrings)

        return substrings

    def _get_main_line_proto_nl(self, answer_node):
        attr_nl = self.nl_provider.get_attribute_nl(answer_node.attribute)
        class_type_nl = self.nl_provider.get_class_type_nl(answer_node.class_type)
        if answer_node.modifier:
            if answer_node.modifier in ("max", "min"):
                mod_nl = self._get_modifier_nl_snippet(answer_node)
                if self.graph.return_attribute:
                    prp_nl = self.nl_provider.get_attribute_nl(
                        self.graph.return_property
                    )
                    main_line = (
                        f"list the {prp_nl} of {class_type_nl} with {mod_nl} {attr_nl} "
                    )
                else:
                    main_line = f"list the {class_type_nl} with {mod_nl} {attr_nl} "
                if answer_node.grounded_entity:  # then act like argmax or argmin
                    mod_nl = self._get_modifier_nl_snippet(answer_node)
                    main_line = f"list all {class_type_nl} with {mod_nl} {attr_nl} "

            elif answer_node.modifier.startswith("count"):
                mod_nl = "many"
                if attr_nl is None:
                    main_line = f"how {mod_nl} {class_type_nl} "
                else:
                    if answer_node.grounded_entity:
                        ge_nl = self.nl_provider.get_grounded_entity_nl(
                            answer_node.grounded_entity
                        )
                        main_line = f"how {mod_nl} {class_type_nl} with {attr_nl} that is {ge_nl} "
                    else:
                        main_line = f"how {mod_nl} {attr_nl} of {class_type_nl} "
            elif answer_node.modifier.startswith("avg"):
                mod_nl = "the average"
                main_line = f"what is {mod_nl} {attr_nl} of {class_type_nl} "
            elif answer_node.modifier.startswith("sum"):
                mod_nl = "the total"
                main_line = f"what is {mod_nl} {attr_nl} of {class_type_nl} "

        else:
            if self.graph.return_attribute:
                prp_nl = self.nl_provider.get_attribute_nl(self.graph.return_property)
                main_line = f"list all {prp_nl} of {class_type_nl} "
            else:
                main_line = f"list all {class_type_nl} "
            if answer_node.grounded_entity:
                ge_nl = self.nl_provider.get_grounded_entity_nl(
                    answer_node.grounded_entity
                )
                main_line += f"with {attr_nl} that is {ge_nl} "

        return main_line

    def _get_modifier_nl_snippet(self, node):
        snippet = ""
        if node.modifier:
            if node.modifier == "max" or node.modifier == "argmax":
                snippet = "the maximum of"
            elif node.modifier == "min" or node.modifier == "argmin":
                snippet = "the minimum of"
            elif node.modifier.startswith("count"):
                snippet = "the count of"
            elif node.modifier.startswith("avg"):
                snippet = "the average of"
            elif node.modifier.startswith("sum"):
                snippet = "the sum of"
            elif node.modifier in (">", "<", "=", "<=", ">="):
                pass
            else:
                raise ValueError("Unknown modifier")
        return snippet

    def _get_modifier_math_nl_snippet(self, node):
        snippet = ""
        if node.modifier:
            if node.modifier.endswith(">"):
                snippet = "greater than"
            elif node.modifier.endswith("<"):
                snippet = "less than"
            elif node.modifier.endswith(">="):
                snippet = "greater or equal than"
            elif node.modifier.endswith("<="):
                snippet = "less or equal than"
            elif node.modifier.endswith("="):
                snippet = "equal to"
            else:
                raise ValueError("Unknown math modifier")
        return snippet

    def _get_relation_proto_nl(self, relation, target_node, in_rel=False):
        rel_line = ""

        if self.use_domain_for_nl_rel_direction:
            in_rel_node_domain = relation.in_node.class_type
        else:
            in_rel_node_domain = None
        if in_rel:
            rel_nl = self.nl_provider.get_inverse_relation_nl(
                relation.rel_type, in_domain=in_rel_node_domain
            )
        else:
            rel_nl = self.nl_provider.get_relation_nl(
                relation.rel_type, in_domain=in_rel_node_domain
            )
        rel_line += rel_nl

        rel_line += " "

        class_type_nl = self.nl_provider.get_class_type_nl(target_node.class_type)
        rel_line += class_type_nl

        if target_node.grounded_entity:
            rel_line += " with "
            if target_node.modifier:
                max_min_nl = self._get_modifier_nl_snippet(target_node)
                rel_line += max_min_nl + " "

                attribute_nl = self.nl_provider.get_attribute_nl(target_node.attribute)
                rel_line += attribute_nl

                if any(target_node.modifier.endswith(x) for x in ("<", ">", "=")):
                    math_nl = self._get_modifier_math_nl_snippet(target_node)
                    rel_line += " " + math_nl + " "
                    ge_nl = self.nl_provider.get_grounded_entity_nl(
                        target_node.grounded_entity
                    )
                    rel_line += f"{ge_nl}"
            else:
                attribute_nl = self.nl_provider.get_attribute_nl(target_node.attribute)
                rel_line += attribute_nl
                rel_line += " that is "
                ge_nl = self.nl_provider.get_grounded_entity_nl(
                    target_node.grounded_entity
                )
                rel_line += f"{ge_nl}"

        return rel_line

    def _recursive_proto_nl(self, relation, from_in=False):
        if not from_in:  # outside relation
            target_node = relation.out_node
            rel_line = self._get_relation_proto_nl(
                relation, target_node, in_rel=from_in
            )
        else:
            target_node = relation.in_node
            rel_line = self._get_relation_proto_nl(
                relation, target_node, in_rel=from_in
            )

        out_nls = []
        for rel in target_node.out_relations:
            if rel != relation:
                out_nls.append(self._recursive_proto_nl(rel, from_in=False))
        in_nls = []
        for rel in target_node.in_relations:
            if rel != relation:
                in_nls.append(self._recursive_proto_nl(rel, from_in=True))

        proto_nl = {
            "nl": rel_line,
            "out_nls": out_nls,
            "in_nls": in_nls,
        }

        return proto_nl

    def write_proto_nl(self):
        answer_node = self.graph.answer_node
        main_line = self._get_main_line_proto_nl(answer_node)

        out_rel_nls = []
        for relation in answer_node.out_relations:
            out_rel_nls.append(self._recursive_proto_nl(relation, from_in=False))

        in_rel_nls = []
        for relation in answer_node.in_relations:
            in_rel_nls.append(self._recursive_proto_nl(relation, from_in=True))

        proto_nl = {
            "nl": main_line,
            "out_nls": out_rel_nls,
            "in_nls": in_rel_nls,
        }

        proto_nl = self._proto_nl_to_str(proto_nl)
        return proto_nl
