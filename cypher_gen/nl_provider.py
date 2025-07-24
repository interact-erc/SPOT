import json


class JsonSchemaCypherNLProvider:
    def __init__(self, schema_path):
        with open(schema_path, "r") as file:
            data = json.load(file)
        self.classes = data["classes"]
        self.relations = data["relations"]
        self.properties = data["properties"]

    def get_relation_nl(self, relation, in_domain=None):
        if in_domain:
            if relation in self.relations:
                if in_domain != self.relations[relation]["domain"]:
                    return self.get_inverse_relation_nl(relation)
        if relation in self.relations:
            return self.relations[relation]["description"]
        return relation

    def get_inverse_relation_nl(self, relation, in_domain=None):
        if in_domain:
            if relation in self.relations:
                if in_domain == self.relations[relation]["domain"]:
                    return self.get_relation_nl(relation)
        if relation in self.relations:
            if "reverse_description" in self.relations[relation]:
                return self.relations[relation]["reverse_description"]
        return f"INVERSE({relation})"

    def get_attribute_nl(self, attribute):
        if attribute in self.properties:
            return self.properties[attribute]["description"]
        return attribute

    def get_grounded_entity_nl(self, entity):
        return entity

    def get_class_type_nl(self, class_type):
        if class_type in self.classes:
            return self.classes[class_type]["description"]
        return class_type
