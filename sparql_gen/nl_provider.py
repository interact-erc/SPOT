import json


class JsonSchemaSPARQLNLProvider:
    def __init__(self, schema_path):
        with open(schema_path, "r") as file:
            data = json.load(file)
        self.classes = data["classes"]
        self.relations = data["relations"]
        self.inverse_relations = data["inverse_relations"]

    def get_relation_nl(self, relation):
        if relation in self.relations:
            return self.relations[relation]["description"]
        if relation in self.inverse_relations:
            inv_rel = self.inverse_relations[relation]
            if "reverse_description" in self.relations[inv_rel]:
                return self.relations[inv_rel]["reverse_description"]
            return f"INVERSE({self.relations[inv_rel]['description']})"
        return relation

    def get_inverse_relation_nl(self, relation):
        if relation in self.inverse_relations:
            inv_rel = self.inverse_relations[relation]
            return self.relations[inv_rel]["description"]
        elif relation in self.relations:
            if "reverse_description" in self.relations[relation]:
                return self.relations[relation]["reverse_description"]
            return f"INVERSE({self.relations[relation]['description']})"
        return f"INVERSE({relation})"

    def get_attribute_nl(self, attribute):
        if attribute in self.classes:
            if "description" in self.classes[attribute]:
                return self.classes[attribute]["description"]
        if attribute == "type.datetime":
            return "date"
        elif attribute == "type.int":
            return "number"
        elif attribute == "type.float":
            return "number"
        return attribute

    def get_grounded_entity_nl(self, entity):
        return entity
