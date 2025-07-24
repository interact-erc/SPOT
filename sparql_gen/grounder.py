import random
import copy
import json
import re
from query_writer import SPARQLQueryWriter
from SPARQLWrapper import SPARQLWrapper, JSON
import urllib
from utils import (
    SPARQL_DATATYPES,
    SPARQL_MODIFIER_NUMBER_TYPES,
    SPARQL_MODIFIER_DATE_TYPES,
)


class SPARQLGraphGrounder:
    def __init__(self, schema_path):
        with open(schema_path, "r") as file:
            data = json.load(file)
        self.classes = list(data["classes"].keys())
        self.relations_info = data["relations"]
        self.relations = list(data["relations"].keys())
        self.inverse_relations = data["inverse_relations"]

        self._build_inv_rel()

        self.max_iterations = 100

        self.filter_english = True

        self.diverse_parallel_relations = False

        self.diverse_sampling = False
        self.diverse_sampling_mode = False
        self.diverse_sampling_mode_classes = False
        self.diverse_sampling_threshold = 0.75
        self.diverse_sampling_threshold_classes = 0.25
        self.sampled_classes = set()
        self.sampled_rel = set()

        self.sparql = SPARQLWrapper("http://127.0.0.1:3001/sparql")
        self.sparql.setReturnFormat(JSON)

        # self.prefix = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> PREFIX : <>"
        self.prefix = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> PREFIX : <http://rdf.freebase.com/ns/>"

    def _build_inv_rel(self):
        new_rel = []
        new_rel_info = {}
        new_inv_rel = {}

        for relation in self.relations_info:
            if "reverse" in self.relations_info[relation]:
                r = self.relations_info[relation]["reverse"]
                new_rel.append(r)
                new_rel_info[r] = {
                    "domain": self.relations_info[relation]["range"],
                    "range": self.relations_info[relation]["domain"],
                    "reverse": relation,
                }
                new_inv_rel[relation] = r
        self.relations = self.relations + new_rel
        self.relations_info.update(new_rel_info)
        self.inverse_relations.update(new_inv_rel)

    def _execute_query(self, query: str):
        query = f"{self.prefix} {query}"

        self.sparql.setQuery(query)
        try:
            results = self.sparql.query().convert()
        except urllib.error.URLError as e:
            raise e
        rtn = []
        for result in results["results"]["bindings"]:
            for var in result:
                result[var] = (
                    result[var]["value"]
                    .replace("http://rdf.freebase.com/ns/", "")
                    .replace("-08:00", "")
                )
            rtn.append(result)
        return rtn

    def _query_label(self, entity):
        query = f"""SELECT ?label
WHERE {{
  :{entity} rdfs:label ?label.
}}
"""
        res = self._execute_query(query)
        if len(res) > 0:
            return res[0]["label"]
        return None

    def _query_datatype(self, relation):
        query = f"""SELECT DISTINCT(DATATYPE(?x) AS ?datatype)
WHERE {{
  ?x0 :{relation} ?x.
}} LIMIT 1"""
        res = self._execute_query(query)
        datatype = res[0]["datatype"]
        if datatype not in SPARQL_DATATYPES:
            raise ValueError(f"Unknown Sparql Datatype: {datatype}")
        return SPARQL_DATATYPES[datatype]

    def _ground_rel(self, relation, rel_type, node_attribute, in_rel=False):
        is_domain = False
        target_attribute = self.relations_info[rel_type]["domain"]
        if self.relations_info[rel_type]["domain"].startswith(node_attribute):
            is_domain = True
            target_attribute = self.relations_info[rel_type]["range"]

        if (is_domain and in_rel) or (not is_domain and not in_rel):
            rel_type = self.inverse_relations[rel_type]

        relation.rel_type = rel_type
        if in_rel:
            relation.in_node.attribute = target_attribute
            return relation.in_node
        else:
            relation.out_node.attribute = target_attribute
            return relation.out_node

    def update_sampled_class_rel(self, graph):
        for rel in graph.relations:
            self.sampled_rel.add(rel.rel_type)
        self.sampled_classes.add(graph.answer_node.attribute)

    def _sample_class(self, classes):
        if self.diverse_sampling_mode_classes:
            cl = [i for i in classes if i not in self.sampled_classes]
            if len(cl) > 0:
                return random.choice(cl)
        return random.choice(classes)

    def _sample_relation(self, relations, k=None):
        if self.diverse_sampling_mode:
            rel = [i for i in relations if i not in self.sampled_rel]
            if len(rel) > 0:
                if k:
                    return random.choices(rel, k=k)
                else:
                    return random.choice(rel)
        if k:
            return random.choices(relations, k=k)
        return random.choice(relations)

    def _get_modifier_suitable_relations(self, attribute, possible_relations, modifier):
        if modifier.startswith("count"):
            return [
                i
                for i in possible_relations
                if (self.relations_info[i]["range"].startswith(attribute))
                or (self.relations_info[i]["domain"].startswith(attribute))
            ]

        allowed_types = [i for i in SPARQL_MODIFIER_NUMBER_TYPES.keys()]
        if not any(modifier.startswith(x) for x in ("sum", "avg")):
            allowed_types += [i for i in SPARQL_MODIFIER_DATE_TYPES.keys()]

        return [
            i
            for i in possible_relations
            if (
                self.relations_info[i]["domain"] in allowed_types
                and self.relations_info[i]["range"].startswith(attribute)
            )
            or (
                self.relations_info[i]["range"] in allowed_types
                and self.relations_info[i]["domain"].startswith(attribute)
            )
        ]

    def _ground_modifier_node_rel(self, node, attribute, possible_relations):
        modifier_nodes = []

        for relation in node.in_relations:
            if relation.rel_type:
                continue
            if relation.in_node.modifier:
                suitable_relations = self._get_modifier_suitable_relations(
                    attribute, possible_relations, relation.in_node.modifier
                )
                if len(suitable_relations) == 0:
                    raise ValueError("Grounder: Unable to find unsuitable relation.")
                rel_type = self._sample_relation(suitable_relations)
                target_node = self._ground_rel(
                    relation, rel_type, attribute, in_rel=True
                )
                modifier_nodes.append(target_node)

        for relation in node.out_relations:
            if relation.rel_type:
                continue
            if relation.out_node.modifier:
                suitable_relations = self._get_modifier_suitable_relations(
                    attribute, possible_relations, relation.out_node.modifier
                )
                if len(suitable_relations) == 0:
                    raise ValueError("Grounder: Unable to find unsuitable relation.")
                rel_type = self._sample_relation(suitable_relations)
                target_node = self._ground_rel(
                    relation, rel_type, attribute, in_rel=False
                )
                modifier_nodes.append(target_node)

        return modifier_nodes

    def _recursive_ground(self, node, modifier_edge=False):
        num_rel = len(node.in_relations) + len(node.out_relations)
        if node.is_answer_node:
            num_rel += 1  # since we do -1 later, but here we start at the root without prior visited relations
        elif num_rel == 1:
            if modifier_edge:
                node.modifier_edge = True
            return True

        attribute = node.attribute
        possible_relations = [
            i
            for i in self.relations
            if self.relations_info[i]["domain"].startswith(attribute)
            or self.relations_info[i]["range"].startswith(attribute)
        ]
        sampled_relations = self._sample_relation(possible_relations, k=num_rel - 1)

        if self.diverse_parallel_relations:
            sampled_relations = list(set(sampled_relations))
            if len(sampled_relations) < (num_rel - 1):
                return False

        modifier_nodes = self._ground_modifier_node_rel(
            node, attribute, possible_relations
        )

        for i in modifier_nodes:
            self._recursive_ground(i, modifier_edge=random.choice([True, False]))

        sample_count = 0
        for relation in node.in_relations:
            if relation.rel_type:
                continue
            rel_type = sampled_relations[sample_count]
            sample_count += 1
            target_node = self._ground_rel(relation, rel_type, attribute, in_rel=True)
            self._recursive_ground(target_node, modifier_edge=modifier_edge)

        for relation in node.out_relations:
            if relation.rel_type:
                continue
            rel_type = sampled_relations[sample_count]
            sample_count += 1
            target_node = self._ground_rel(relation, rel_type, attribute, in_rel=False)
            self._recursive_ground(target_node, modifier_edge=modifier_edge)

    def edit_query(self, query, edge_nodes, modifier_nodes):
        for i in range(len(edge_nodes)):
            query = query.replace(f":?y{i}", f"?y{i}")
        for node in modifier_nodes:
            query = query.replace(f"?x{node.id}", f"?z{node.id}")
        query_first_line = " ".join(
            [i.grounded_entity for i in edge_nodes]
            + [f"({i.grounded_entity} as ?z{i.id})" for i in modifier_nodes]
        )

        query_first_line = "SELECT DISTINCT " + "?x0 " + query_first_line
        query = query.replace("SELECT COUNT DISTINCT", "SELECT DISTINCT")
        query = query.replace("SUM(?x0)", "?x0")
        query = query.replace("AVG(?x0)", "?x0")
        query = query.replace("SELECT DISTINCT ?x0", query_first_line)
        if self.filter_english:
            filter_lines = [
                "FILTER (!isLiteral(?x0) OR lang(?x0) = '' OR langMatches(lang(?x0), 'en'))"
            ]
            for i in edge_nodes:
                gr_e = i.grounded_entity
                filter_lines.append(
                    f"FILTER (!isLiteral({gr_e}) OR lang({gr_e}) = '' OR langMatches(lang({gr_e}), 'en'))"
                )
            for i in modifier_nodes:
                filter_lines.append(
                    f"FILTER (!isLiteral(?z{i.id}) OR lang(?z{i.id}) = '' OR langMatches(lang(?z{i.id}), 'en'))"
                )
            filter_line = "WHERE {\n" + "\n".join(filter_lines)
            query = query.replace("WHERE {", filter_line)
        query = query.replace("LIMIT 1", "")

        group_by_pos = query.upper().find("GROUP BY")
        if group_by_pos != -1:
            query = query[:group_by_pos]

        pattern = (
            r'\s*FILTER\s*\(\?[\w\d_]+\s*(>=|<=|<|>|=)\s*[\?\w\d_:"\^\^<>\s]+\)\s*?'
        )
        query = re.sub(pattern, "", query)

        return query

    def ground_graph(self, original_graph):

        iter = 0
        grounded = False
        while not grounded and iter < self.max_iterations:
            if self.diverse_sampling:
                self.diverse_sampling_mode = True
                if iter > self.diverse_sampling_threshold * self.max_iterations:
                    self.diverse_sampling_mode = False
                self.diverse_sampling_mode_classes = True
                if iter > self.diverse_sampling_threshold_classes * self.max_iterations:
                    self.diverse_sampling_mode_classes = False

            iter += 1
            graph = copy.deepcopy(original_graph)
            answer_node = graph.answer_node
            if answer_node.modifier and answer_node.modifier != "count":
                answer_node.modifier_edge = random.choice([True, False])
                possible_classes = [
                    i for i in self.classes if i in SPARQL_MODIFIER_NUMBER_TYPES
                ]
                if answer_node.modifier not in ("sum", "avg"):
                    possible_classes += [
                        i for i in self.classes if i in SPARQL_MODIFIER_DATE_TYPES
                    ]
                answer_node_class = self._sample_class(possible_classes)
            else:
                answer_node_class = self._sample_class(self.classes)
            answer_node.attribute = answer_node_class

            try:
                self._recursive_ground(
                    answer_node, modifier_edge=answer_node.modifier_edge
                )
            except Exception as e:
                continue

            edge_nodes = []
            for node in graph.nodes:
                if (
                    (not node.is_answer_node)
                    and (len(node.in_relations) + len(node.out_relations) == 1)
                    and (not node.modifier)
                ):
                    node.grounded_entity = f"?y{len(edge_nodes)}"
                    edge_nodes.append(node)
            modifier_nodes = []
            for node in graph.nodes:
                if (
                    (not node.is_answer_node)
                    and node.modifier
                    and any(mod in node.modifier for mod in ("=", "<", ">"))
                ):
                    if node.modifier.startswith("avg"):
                        node.grounded_entity = f"AVG(?z{node.id})"
                    elif node.modifier.startswith("sum"):
                        node.grounded_entity = f"SUM(?z{node.id})"
                    elif node.modifier.startswith("count"):
                        node.grounded_entity = f"COUNT(?z{node.id})"
                    else:
                        node.grounded_entity = f"?z{node.id}"
                    modifier_nodes.append(node)

            sparql_writer = SPARQLQueryWriter(graph)
            sparql_query = sparql_writer.write_query()
            sparql_query = self.edit_query(sparql_query, edge_nodes, modifier_nodes)
            try:
                results = self._execute_query(sparql_query)
            except Exception as e:
                results = []
            if len(results) > 0:
                grounded = True
                res = random.choice(results)
                for i, node in enumerate(edge_nodes):
                    entity = res[f"y{i}"]
                    if node.modifier_edge:
                        node.grounded_entity = None
                    elif not node.attribute.startswith("type"):
                        entity_label = self._query_label(entity)
                        if entity_label is None:
                            grounded = False
                        else:
                            graph.entity_mapping[entity_label] = entity
                            node.grounded_entity = entity_label
                    else:
                        datatype = self._query_datatype(node.in_relations[0].rel_type)
                        node.grounded_entity = entity
                        node.datatype = datatype
                for node in modifier_nodes:
                    entity = res[f"z{node.id}"]
                    if node.modifier[-1] == "<" or node.modifier[-1] == ">":
                        try:
                            entity = int(entity)
                            if node.modifier[-1] == "<":
                                entity = entity + 1
                            else:
                                entity = entity - 1
                            entity = str(entity)
                        except:
                            pass
                    node.grounded_entity = entity
                    try:
                        datatype = self._query_datatype(node.in_relations[0].rel_type)
                    except:
                        datatype = None
                    node.datatype = datatype

        return graph, grounded
