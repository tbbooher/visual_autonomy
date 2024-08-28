# filename: print_levels.py

import pandas as pd
import networkx as nx

def build_graph(df, id_to_name):
    """Build a directed graph from the DataFrame of programs and their dependencies."""
    G = nx.DiGraph()
    
    for _, row in df.iterrows():
        program_name = row['Program Name']
        dependencies = str(row['Dependency']).split(',') if pd.notna(row['Dependency']) else []
        
        G.add_node(program_name)  # Add program as a node
        
        for dependency in dependencies:
            dependency = dependency.strip()
            if dependency.isdigit():
                dependency_name = id_to_name.get(dependency, None)
                if dependency_name:
                    G.add_edge(dependency_name, program_name)  # Add directed edge from dependency to program
    return G

def print_program_levels(graph):
    """Print the program levels starting from initial programs and moving towards final programs."""
    # Find all source programs (nodes with no predecessors)
    start_programs = [node for node in graph.nodes if graph.in_degree(node) == 0]
    levels_dict = {}

    def traverse_program(program, level):
        if program in levels_dict and levels_dict[program] >= level:
            return
        
        levels_dict[program] = level
        print(f"{'  ' * (level - 1)}Level {level}: {program}")

        for successor in graph.successors(program):
            traverse_program(successor, level + 1)

    for start_program in start_programs:
        traverse_program(start_program, 1)

def main():

    id_to_name = pd.Series(df['Program Name'].values, index=df['ID']).to_dict()
    
    G = build_graph(df, id_to_name)
    print_program_levels(G)

if __name__ == "__main__":
    main()
