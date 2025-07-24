from graph import Graph
from utils import (
    SPARQL_MODIFIER_NUMBER_TYPES,
    SPARQL_MODIFIER_DATE_TYPES,
)


class SPARQLQueryWriter:

    def __init__(self, graph: Graph):
        self.graph = graph
        self.nl_provider = None

    def set_nl_provider(self, provider):
        self.nl_provider = provider

    def _map_query_entities(self, query):
        lines = query.split("\n")
        for i, line in enumerate(lines):
            if line.lstrip().startswith((":", "?")):
                for key in sorted(
                    self.graph.entity_mapping.keys(), key=len, reverse=True
                ):
                    line = line.replace(key, self.graph.entity_mapping[key])
            lines[i] = line
        return "\n".join(lines)

    def _post_edit_non_literals(self, query):
        for node in self.graph.nodes:
            if node.datatype:
                query = query.replace(
                    f":{node.grounded_entity}",
                    f'"{node.grounded_entity}"^^{node.datatype}',
                )
        return query

    def _get_select_variable_line(self):
        answer_node = self.graph.answer_node
        select_line = "SELECT DISTINCT"
        variable_line = f"?x{answer_node.id}"
        if answer_node.modifier:
            modifier = answer_node.modifier
            if modifier == "count":
                select_line = "SELECT COUNT DISTINCT"
                variable_line = f"?x{answer_node.id}"
            elif modifier == "sum":
                variable_line = f"SUM(?x{answer_node.id})"
            elif modifier == "avg":
                variable_line = f"AVG(?x{answer_node.id})"

        line = f"{select_line} {variable_line}"
        return [line, "WHERE {"]

    def _get_node_variable_or_entity(self, node):
        if node.grounded_entity and not (
            node.modifier
            and any(
                node.modifier.startswith(prefix)
                for prefix in ("count", "sum", "avg", "<", ">", "=")
            )
        ):
            return f":{node.grounded_entity}", {"node": f":{node.grounded_entity}"}
        else:
            return f"?x{node.id}", {"node": f"?x{node.id}"}

    def _get_triplet_line(self, relation):
        obj, _ = self._get_node_variable_or_entity(relation.in_node)
        rel = relation.rel_type
        subj, _ = self._get_node_variable_or_entity(relation.out_node)
        line = f"{obj} :{rel} {subj} ."
        return [line]

    def _get_attribute_line(self, node):
        lines = []
        if node.attribute:
            attr, _ = self._get_node_variable_or_entity(node)
            line = f"{attr} a :{node.attribute} ."
            lines.append(line)
        return lines

    def _get_modifier_filter_line(self, node):
        lines = []
        line = f'FILTER (?x{node.id} {node.modifier} "{node.grounded_entity}"^^{node.datatype})'
        lines.append(line)
        return lines

    def _get_modifier_line(self, node):
        lines = []
        nd, _ = self._get_node_variable_or_entity(node)
        if not node.modifier:
            pass
        elif node.modifier == "max":
            lines.append(f"ORDER BY DESC({nd})")
            lines.append("LIMIT 1")
        elif node.modifier == "min":
            lines.append(f"ORDER BY {nd}")
            lines.append("LIMIT 1")
        elif node.modifier in ("count", "sum", "avg"):
            pass
        elif node.modifier.startswith("count"):
            lines.append(f"GROUP BY ?x{self.graph.answer_node.id}")
            lines.append(
                f"HAVING (COUNT({nd}) {node.modifier[len('count'):]} {node.grounded_entity})"
            )
        elif node.modifier.startswith("sum"):
            lines.append(f"GROUP BY ?x{self.graph.answer_node.id}")
            lines.append(
                f"HAVING (SUM({nd}) {node.modifier[len('sum'):]} {node.grounded_entity})"
            )
        elif node.modifier.startswith("avg"):
            lines.append(f"GROUP BY ?x{self.graph.answer_node.id}")
            lines.append(
                f"HAVING (AVG({nd}) {node.modifier[len('avg'):]} {node.grounded_entity})"
            )
        return lines

    def write_query(self, map_entities=True):
        query_lines = []

        q_l = self._get_select_variable_line()
        query_lines += q_l

        for relation in self.graph.relations:
            q_l = self._get_triplet_line(relation)
            query_lines += q_l

        for node in self.graph.nodes:
            if node.is_answer_node:
                if (
                    node.attribute not in SPARQL_MODIFIER_NUMBER_TYPES
                    and node.attribute not in SPARQL_MODIFIER_DATE_TYPES
                ):
                    q_l = self._get_attribute_line(node)
                    query_lines += q_l

        for node in self.graph.nodes:
            if any(node.modifier == x for x in (">=", "<=", "<", ">", "=")):
                q_l = self._get_modifier_filter_line(node)
                query_lines += q_l

        query_lines += ["}"]

        for node in self.graph.nodes:
            if node.modifier:
                q_l = self._get_modifier_line(node)
                query_lines += q_l

        query = "\n".join(query_lines)
        if map_entities:
            query = self._map_query_entities(query)

        query = self._post_edit_non_literals(query)

        return query

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

    def _proto_nl_to_str(self, proto):
        main_line = proto["nl"]

        substrings = []
        for out in proto.get("out_nls", []):
            out_strings = self._extract_recursive_proto_nls(out)
            substrings += out_strings
        for inn in proto.get("in_nls", []):
            in_strings = self._extract_recursive_proto_nls(inn)
            substrings += in_strings

        substrings = [main_line + " " + i for i in substrings]
        substrings = "\nAND ".join(substrings)

        return substrings

    def _get_main_line_proto_nl(self, answer_node):
        attr_nl = self.nl_provider.get_attribute_nl(answer_node.attribute)
        if answer_node.modifier:
            if answer_node.modifier in ("max", "min"):
                mod_nl = self._get_modifier_nl_snippet(answer_node)
                main_line = f"list {mod_nl} {attr_nl} "
            elif answer_node.modifier.startswith("count"):
                mod_nl = "many"
                main_line = f"how {mod_nl} {attr_nl} "
            elif answer_node.modifier.startswith("avg"):
                mod_nl = "the average"
                main_line = f"what is {mod_nl} {attr_nl} "
            elif answer_node.modifier.startswith("sum"):
                mod_nl = "the total"
                main_line = f"what is {mod_nl} {attr_nl} "
        else:
            main_line = f"list all {attr_nl} "

        return main_line

    def _get_modifier_nl_snippet(self, node):
        snippet = ""
        if node.modifier:
            if node.modifier == "max":
                snippet = "the maximum of"
            elif node.modifier == "min":
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

        if in_rel:
            rel_nl = self.nl_provider.get_inverse_relation_nl(relation.rel_type)
        else:
            rel_nl = self.nl_provider.get_relation_nl(relation.rel_type)
        rel_line += rel_nl

        rel_line += ""

        max_min_nl = self._get_modifier_nl_snippet(target_node)
        rel_line += " " + max_min_nl + (" " if max_min_nl else "")

        attr_nl = self.nl_provider.get_attribute_nl(target_node.attribute)
        rel_line += attr_nl

        if target_node.grounded_entity:
            rel_line += " which is "
            if target_node.modifier and any(
                target_node.modifier.endswith(x) for x in ("<", ">", "=")
            ):
                math_nl = self._get_modifier_math_nl_snippet(target_node)
                rel_line += math_nl + " "

            ge_nl = self.nl_provider.get_grounded_entity_nl(target_node.grounded_entity)
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
