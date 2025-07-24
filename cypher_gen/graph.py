MODIFIER_MAPPINGS = {
    "max": "A",
    "min": "B",
    "count": "C",
    "count<": "D",
    "count=": "E",
    "count>": "F",
    "count>=": "G",
    "count<=": "H",
    "avg": "I",
    "avg<": "J",
    "avg=": "K",
    "avg>": "L",
    "avg>=": "M",
    "avg<=": "N",
    "sum": "O",
    "sum<": "P",
    "sum=": "Q",
    "sum>": "R",
    "sum>=": "S",
    "sum<=": "T",
    "<": "U",
    ">": "V",
    "=": "W",
    "<=": "X",
    ">=": "Y",
}


class Node:
    def __init__(self, id, is_answer_node=False):
        self.id = id
        self.type = None
        self.is_answer_node = is_answer_node
        self.grounded_entity = None
        self.attribute = None
        self.class_type = None
        self.in_relations = []
        self.out_relations = []
        self.modifier_edge = False
        self.modifier = None
        self.datatype = None


class Relation:
    def __init__(self, in_node, out_node):
        self.in_node = in_node
        self.out_node = out_node
        self.rel_type = None


class Graph:
    def __init__(self):
        node = Node(0, is_answer_node=True)
        self.answer_node = node
        self.nodes = []
        self.nodes.append(self.answer_node)
        self.relations = []
        self.entity_mapping = {}
        self.return_attribute = False  # return x0 if False, x0.property if True
        self.return_property = None

    def add_in_relation(self, node_id, rel_type=None):
        in_node = Node(len(self.nodes))
        self.nodes.append(in_node)
        out_node = [node for node in self.nodes if node.id == node_id][0]
        relation = Relation(in_node, out_node)
        if rel_type:
            relation.rel_type = rel_type
        self.relations.append(relation)
        in_node.out_relations.append(relation)
        out_node.in_relations.append(relation)
        return in_node.id

    def add_out_relation(self, node_id, rel_type=None):
        out_node = Node(len(self.nodes))
        self.nodes.append(out_node)
        in_node = [node for node in self.nodes if node.id == node_id][0]
        relation = Relation(in_node, out_node)
        if rel_type:
            relation.rel_type = rel_type
        self.relations.append(relation)
        in_node.out_relations.append(relation)
        out_node.in_relations.append(relation)
        return out_node.id

    def set_attribute(self, node_id, attribute):
        self.nodes[node_id].attribute = attribute

    def set_grounded_entity(self, node_id, grounded_entity):
        self.nodes[node_id].grounded_entity = grounded_entity

    def set_relation(self, rel_id, relation):
        self.relations[rel_id].rel_type = relation

    def set_modifier(self, node_id, modifier):
        assert modifier in MODIFIER_MAPPINGS
        self.nodes[node_id].modifier = modifier

    def ground_anonymously(self):
        for node in self.nodes:
            if (
                (not node.is_answer_node)
                and (len(node.in_relations + node.out_relations) == 1)
                and (not node.modifier)
            ):
                if not node.grounded_entity:
                    node.grounded_entity = f"E{node.id}"
                if not node.attribute:
                    node.attribute = f"A{node.id}"
            if not node.attribute:
                node.attribute = f"A{node.id}"
            if not node.class_type:
                node.class_type = f"T{node.id}"
            if node.modifier:
                node.datatype = "FLOAT"
                if any(
                    node.modifier.startswith(prefix)
                    for prefix in ("count", "sum", "avg", "<", ">", "=")
                ):
                    node.grounded_entity = f"E{node.id}"
        for i, relation in enumerate(self.relations):
            if not relation.rel_type:
                relation.rel_type = f"R{i}"

    def get_matrix_representation(self):
        matrix = [["0" for _ in self.nodes] for _ in self.nodes]
        for relation in self.relations:
            m_entry = "1"
            if relation.in_node.modifier:
                m_entry = MODIFIER_MAPPINGS[relation.in_node.modifier] + m_entry
            if relation.out_node.modifier:
                m_entry = m_entry + MODIFIER_MAPPINGS[relation.out_node.modifier]
            matrix[relation.in_node.id][relation.out_node.id] = m_entry

        return matrix
