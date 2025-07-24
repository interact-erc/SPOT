import random
import copy
import json
from query_writer import CypherQueryWriter
from utils import (
    Neo4jConnection,
    CYPHER_MODIFIER_DATE_TYPES,
    CYPHER_MODIFIER_NUMBER_TYPES,
)
from itertools import combinations


class CypherGraphGrounder:
    def __init__(self, schema_path):
        with open(schema_path, "r") as file:
            data = json.load(file)
        self.classes = list(data["classes"].keys())
        self.relations_info = data["relations"]
        self.relations = list(data["relations"].keys())
        self.properties = list(data["properties"].keys())
        self.properties_info = data["properties"]
        self.max_iterations = 100

        self.diverse_sampling = True

        self.diverse_parallel_relations = False

        self.pattern_rel_cache = (
            set()
        )  # Set of patterns with instantiated rels that we have tried to ground already

        uri = "bolt://localhost:7687"
        user = "neo4j"
        password = "neo4jneo4j"
        self.neo4j = Neo4jConnection(uri, user, password)

    def _execute_query(self, query: str):
        results = self.neo4j.query(query)
        return results

    def edit_query(self, query, graph):
        query = query.split("\n")
        query = [i for i in query if not i.startswith("WHERE")]
        query = [i for i in query if not i.startswith("WITH")]
        query = [i for i in query if not i.startswith("ORDER")]

        added_return_variables = "RETURN x0"
        for node in graph.nodes[1:]:
            added_return_variables += f", x{node.id}"
        query = [added_return_variables if i.startswith("RETURN") else i for i in query]

        query = "\n".join(query)
        query = query.replace("LIMIT 1", "")
        return query

    def _ground_rel(self, relation, rel_type, in_rel=False):
        relation.rel_type = rel_type
        if in_rel:
            target_class = self.relations_info[rel_type]["domain"]
            relation.in_node.class_type = target_class
            return relation.in_node
        else:
            target_class = self.relations_info[rel_type]["range"]
            relation.out_node.class_type = target_class
            return relation.out_node

    def _recursive_ground(self, node, modifier_edge=False):
        succesfull_ground = True
        num_in_rel = len(node.in_relations)
        num_out_rel = len(node.out_relations)
        if num_in_rel + num_out_rel == 1 and not node.is_answer_node:
            if modifier_edge:
                node.modifier_edge = True
            return True

        class_type = node.class_type
        possible_out_relations = [
            i
            for i in self.relations
            if self.relations_info[i]["domain"].startswith(class_type)
        ]
        possible_in_relations = [
            i
            for i in self.relations
            if self.relations_info[i]["range"].startswith(class_type)
        ]

        if possible_in_relations:
            sampled_in_relations = random.choices(possible_in_relations, k=num_in_rel)
        if possible_out_relations:
            sampled_out_relations = random.choices(
                possible_out_relations, k=num_out_rel
            )

        if self.diverse_parallel_relations:
            sampled_in_relations = list(set(sampled_in_relations))
            if len(sampled_in_relations) < (num_in_rel):
                return False
            sampled_out_relations = list(set(sampled_out_relations))
            if len(sampled_out_relations) < (num_out_rel):
                return False

        sample_in_count = 0
        for relation in node.in_relations:
            if relation.rel_type:
                # relation is already grounded (is the edge where we came from in the exploration)
                continue
            if not possible_in_relations:
                return False
            rel_type = sampled_in_relations[sample_in_count]
            sample_in_count += 1
            target_node = self._ground_rel(relation, rel_type, in_rel=True)
            mod_edge = modifier_edge
            if target_node.modifier:
                target_node.modifier_edge = random.choice([True, False])
                mod_edge = target_node.modifier_edge
            succesfull_ground = succesfull_ground and self._recursive_ground(
                target_node, modifier_edge=mod_edge
            )

        sample_out_count = 0
        for relation in node.out_relations:
            if relation.rel_type:
                # relation is already grounded (is the edge where we came from in the exploration)
                continue
            if not possible_out_relations:
                return False
            rel_type = sampled_out_relations[sample_out_count]
            sample_out_count += 1
            target_node = self._ground_rel(relation, rel_type, in_rel=False)
            mod_edge = modifier_edge
            if target_node.modifier:
                target_node.modifier_edge = random.choice([True, False])
                mod_edge = target_node.modifier_edge
            succesfull_ground = succesfull_ground and self._recursive_ground(
                target_node, modifier_edge=mod_edge
            )

        return succesfull_ground

    def _pick_random_property(self, res, exclude=[]):
        candidates = []
        for i in res.keys():
            if res[i] is not None and i not in ("keywords", "label"):
                if i not in exclude:
                    candidates.append((i, res[i]))
        candidates = [i for i in candidates if i[0] in self.properties]
        cand = random.choice(candidates)
        return cand[0], cand[1]

    def _pick_modifier_property(self, res, modifier):
        candidates = []
        if modifier.startswith("count"):
            for i in res.keys():
                if res[i] is not None and i not in ("keywords", "label"):
                    if i in self.properties:
                        candidates.append((i, res[i], self.properties_info[i]["type"]))
        else:
            types = [i for i in CYPHER_MODIFIER_NUMBER_TYPES.keys()]
            if not any(modifier.startswith(x) for x in ("sum", "avg")):
                types += [i for i in CYPHER_MODIFIER_DATE_TYPES.keys()]
            for i in res.keys():
                if res[i] is not None and i not in ("keywords", "label"):
                    if i in self.properties:
                        if self.properties_info[i]["type"] in types:
                            candidates.append(
                                (i, res[i], self.properties_info[i]["type"])
                            )
        if len(candidates) == 0:
            return None, None, False, None
        candidates = [i for i in candidates if i[0] in self.properties]
        cand = random.choice(candidates)
        return cand[0], cand[1], True, cand[2]

    def _get_list_modifier_properties(self, modifier):
        candidates = []
        types = [i for i in CYPHER_MODIFIER_NUMBER_TYPES.keys()]
        if modifier not in ("sum", "avg"):
            types += [i for i in CYPHER_MODIFIER_DATE_TYPES.keys()]
        for i in self.properties_info.keys():
            if self.properties_info[i]["type"] in types:
                candidates.append(i)
        return candidates

    def _get_nodes_combinations(self, graph):
        number = len(graph.nodes)
        result = []
        result.append(())
        for r in range(1, number + 1):
            for combo in combinations(range(0, number), r):
                result.append(combo)
        return result

    def ground_graph(self, original_graph):
        grounded_graphs = []
        iter = 0
        grounded = False
        while not grounded and iter < self.max_iterations:
            iter += 1
            graph = copy.deepcopy(original_graph)
            answer_node = graph.answer_node

            answer_node_class = random.choice(self.classes)
            answer_node.class_type = answer_node_class
            if answer_node.modifier and answer_node.modifier != "count":
                answer_node.modifier_edge = random.choice([True, False])

            was_grounded = self._recursive_ground(
                answer_node, modifier_edge=answer_node.modifier_edge
            )

            if not was_grounded:
                continue
            else:
                cypher_writer = CypherQueryWriter(graph)
                anon_query = cypher_writer.write_query()
                if anon_query in self.pattern_rel_cache:
                    continue
                else:
                    self.pattern_rel_cache.add(anon_query)

            cypher_writer = CypherQueryWriter(graph)
            cypher_query = cypher_writer.write_query()
            cypher_query = self.edit_query(cypher_query, graph)

            try:
                results = self._execute_query(cypher_query)
            except Exception as e:

                results = []

            if len(results) > 0:
                nodes_id_to_ground = self._get_nodes_combinations(graph)
                if not graph.nodes[0].modifier:
                    modifier_nodes_id = [
                        x for x in range(len(graph.nodes)) if graph.nodes[x].modifier
                    ]
                    nodes_id_to_ground = [
                        tup
                        for tup in nodes_id_to_ground
                        if all(num in tup for num in modifier_nodes_id)
                    ]
                elif graph.nodes[0].modifier in ("max", "min"):
                    nodes_id_to_ground = [
                        tup for tup in nodes_id_to_ground if 0 not in tup
                    ]

                to_return_attr = [False for _ in nodes_id_to_ground]
                if not graph.nodes[0].modifier or graph.nodes[0].modifier in (
                    "min",
                    "max",
                ):
                    to_return_attr += [True for _ in nodes_id_to_ground]
                    nodes_id_to_ground += nodes_id_to_ground

                for ntg, t_r_a in zip(nodes_id_to_ground, to_return_attr):
                    ntg_grounded = True
                    graph_tg = copy.deepcopy(graph)

                    graph_tg.return_attribute = t_r_a

                    res = random.choice(results)
                    for node_id in ntg:
                        node = graph_tg.nodes[node_id]

                        if node.is_answer_node:
                            if node.modifier:
                                x0_property, x_ent, grnd, x0_datatype = (
                                    self._pick_modifier_property(
                                        res["x0"], node.modifier
                                    )
                                )
                                ntg_grounded = ntg_grounded and grnd
                                graph.nodes[0].datatype = x0_datatype
                            else:
                                x0_property, x_ent = self._pick_random_property(
                                    res["x0"]
                                )
                            node.grounded_entity = x_ent
                            node.attribute = x0_property
                        else:
                            if node.modifier:
                                x_prop, x_ent, grnd, x_datatype = (
                                    self._pick_modifier_property(
                                        res[f"x{node.id}"], node.modifier
                                    )
                                )
                                if node.modifier.startswith("count") and len(
                                    node.modifier
                                ) > len("count"):
                                    x_ent = res[f"z{node.id}"]
                                ntg_grounded = ntg_grounded and grnd
                                node.datatype = x_datatype
                                if node.modifier[-1] == "<" or node.modifier[-1] == ">":
                                    try:
                                        x_ent = int(x_ent)
                                        if node.modifier[-1] == "<":
                                            x_ent = x_ent + 1
                                        else:
                                            x_ent = x_ent - 1
                                        x_ent = str(x_ent)
                                    except:
                                        pass
                            else:
                                x_prop, x_ent = self._pick_random_property(
                                    res[f"x{node.id}"]
                                )
                            node.grounded_entity = x_ent
                            node.attribute = x_prop

                    if 0 not in ntg:
                        node = graph_tg.nodes[0]
                        if node.modifier and not node.modifier.startswith("count"):
                            x0_property, _, grnd, x0_datatype = (
                                self._pick_modifier_property(res["x0"], node.modifier)
                            )
                            ntg_grounded = ntg_grounded and grnd
                            graph.nodes[0].datatype = x0_datatype
                            node.attribute = x0_property

                    # If needs to return property choose it here. Property must not be the same as grounded attribute
                    if t_r_a:
                        to_exclude = []
                        if graph_tg.nodes[0].attribute is not None:
                            if graph_tg.nodes[0].modifier not in (
                                "count",
                                "max",
                                "min",
                            ):
                                to_exclude = [graph_tg.nodes[0].attribute]
                        if len(res["x0"].keys()) > len(to_exclude):
                            x0_property, _ = self._pick_random_property(
                                res["x0"], exclude=to_exclude
                            )
                            graph_tg.return_property = x0_property
                        else:
                            ntg_grounded = False

                    if ntg_grounded:
                        grounded = True
                        grounded_graphs.append(graph_tg)

        return grounded_graphs, grounded
