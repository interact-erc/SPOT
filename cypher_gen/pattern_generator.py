import random
from graph import Graph
import itertools
import copy


class PatternGenerator:
    def __init__(self):
        self.max_nodes = 5
        self.max_new_edges = 3
        self.add_modifiers = True
        self.max_pattern_trials = 100000
        self.remove_modifiers = []

    def matrix_to_string(self, m):
        m = [str(item) for row in m for item in row]
        m = ".".join(m)
        return m

    def _is_pattern_unique(self, pattern, pattern_set):
        # Note: This does not work correctly for graph with modifiers
        matrix = pattern.get_matrix_representation()

        numbers = list(range(1, len(matrix)))
        permutations = list(itertools.permutations(numbers))
        permutations = [(0,) + perm for perm in permutations]
        matrixes = []
        for permutation in permutations:
            m = [[0 for _ in matrix] for _ in matrix]
            for i in range(len(matrix)):
                for j in range(len(matrix)):
                    m[i][j] = matrix[permutation[i]][permutation[j]]
                    if (i > j and permutation[i] < permutation[j]) or (
                        i < j and permutation[i] > permutation[j]
                    ):
                        m[i][j] = m[i][j][::-1]
            m = self.matrix_to_string(m)
            matrixes.append(m)

        matrix_string = self.matrix_to_string(matrix)
        is_unique = True
        for i in matrixes:
            if i in pattern_set:
                is_unique = False
        return is_unique, matrix_string

    def _generate_pattern(self):
        graph = Graph()

        num_nodes = random.randint(2, self.max_nodes)
        node_ids_to_explore = [0]

        while len(node_ids_to_explore) > 0 and len(graph.nodes) < num_nodes:
            target_node = random.choice(node_ids_to_explore)
            node_ids_to_explore.remove(target_node)
            max_new_edges = min(self.max_new_edges, num_nodes - len(graph.nodes))
            num_new_edges = random.randint(0, max_new_edges)
            if num_new_edges == 0 and target_node == 0:
                num_new_edges = 1
            for _ in range(num_new_edges):
                if random.randint(0, 1) == 0:
                    node_id = graph.add_in_relation(target_node)
                else:
                    node_id = graph.add_out_relation(target_node)
                node_ids_to_explore.append(node_id)

        return graph

    def _get_modifiers(self, is_answer_node):
        if is_answer_node:
            modifiers = ["max", "min", "count", "sum", "avg"]
            modifiers = [
                elem for elem in modifiers if elem not in self.remove_modifiers
            ]
            return modifiers
        else:
            modifiers = []

            return modifiers

    def generate_patterns(self, num_patterns, unique=False):
        patterns = []
        patterns_mod = []
        pattern_encodings = set()
        trial_counts = 0
        while len(patterns) < num_patterns and trial_counts < self.max_pattern_trials:
            trial_counts += 1
            pattern = self._generate_pattern()

            if unique:
                is_pattern_unique, matrix_encoding = self._is_pattern_unique(
                    pattern, pattern_encodings
                )
                if is_pattern_unique:
                    pattern_encodings.add(matrix_encoding)
                    patterns.append(pattern)

                    if self.add_modifiers:
                        for node in pattern.nodes:
                            for mod in self._get_modifiers(node.is_answer_node):
                                graph = copy.deepcopy(pattern)
                                graph.set_modifier(node.id, mod)
                                patterns_mod.append(graph)

            else:
                patterns.append(pattern)

                if self.add_modifiers:
                    for node in pattern.nodes:
                        for mod in self._get_modifiers(node.is_answer_node):
                            graph = copy.deepcopy(pattern)
                            graph.set_modifier(node.id, mod)
                            patterns_mod.append(graph)

        random.shuffle(patterns_mod)
        patterns = patterns + patterns_mod

        return patterns
