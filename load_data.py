import os
import gzip
from copy import deepcopy
from parse_format import parse_format
from utils import match_re_global, match_re_nodes, convert_attribute, convert
from py2neo import Graph


def importNeo(uri, user, password, nodes_and_edges, node_primary_key):
    """
    Import data into neo4j database

    Args:
        uri: Neo4j uri
        user: Neo4j user name
        password: database password
        nodes_and_edges: dict of node and edge info of one line in datafile (from load_tabular_data function)
        node_primary_key: primary keys of each node (from load_tabular_data function)
    """
    graph = Graph(uri, user=user, password=password)

    # merge node
    for node in node_primary_key.keys(): #
        cypher = "MERGE (n:" + nodes_and_edges[node]['label'] + " {"
        onset = "n.add_time = datetime(), "

        # merge on primary keys
        for attr in node_primary_key[node]:
            if nodes_and_edges[node][attr][1] == 'str':
                cypher = cypher + attr + ": \"" + nodes_and_edges[node][attr][0] + "\", "
            else:
                cypher = cypher + attr + ": " + str(nodes_and_edges[node][attr][0]) + ", "

        # on merge renew or create other attributes
        for attr in nodes_and_edges[node].keys():
            if attr != 'node_or_edge' and attr != 'label' and attr not in node_primary_key[node]:
                if nodes_and_edges[node][attr][1] == 'str':
                    onset = onset + "n." + attr + "= \"" + nodes_and_edges[node][attr][0] + "\", "
                else:
                    onset = onset + "n." + attr + "= " + str(nodes_and_edges[node][attr][0]) + ", "

        cypher = cypher[:-2] + "} ) ON CREATE SET " + onset[:-2] + " ON MATCH SET " + onset[:-2]

        graph.run(cypher=cypher)

    # merge edge
    for edge in nodes_and_edges.keys():
        if nodes_and_edges[edge]['node_or_edge'] == 'node':
            continue
        node1 = nodes_and_edges[edge]['from']
        node2 = nodes_and_edges[edge]['to']

        cypher = "MATCH (n:" + nodes_and_edges[node1]['label'] + "), (a:" + nodes_and_edges[node2]['label'] + ") WHERE "
        for node, n in zip([node1, node2], ['n.', 'a.']):
            for attr in node_primary_key[node]:
                if nodes_and_edges[node][attr][1] == 'str':
                    cypher = cypher + n + attr + " = \"" + nodes_and_edges[node][attr][0] + "\" AND "
                else:
                    cypher = cypher + n + attr + " = " + str(nodes_and_edges[node][attr][0]) + " AND "

        edgepropstr = " {"
        exist = False # check if edge property do exist
        for attr in nodes_and_edges[edge].keys():
            if attr != 'node_or_edge' and attr != 'label' and attr != 'from' and attr != 'to':
                exist = True
                if nodes_and_edges[edge][attr][1] == 'str':
                    edgepropstr += attr + ": \"" + nodes_and_edges[edge][attr][0] + "\", "
                else:
                    edgepropstr += attr + ": " + str(nodes_and_edges[edge][attr][0]) + ", "
        edgepropstr = edgepropstr[:-2] + "}"

        if not exist:
            cypher = cypher[:-4] + "MERGE (n) - [:" + nodes_and_edges[edge]['label'] + "] -> (a)"
        else:
            cypher = cypher[:-4] + "MERGE (n) - [:" + nodes_and_edges[edge]['label'] + edgepropstr + "] -> (a)"

        graph.run(cypher=cypher)


def load_tabular_data(format_file, data_files, uri, user, password):
    """
    Load tabular data

    Args:
        format_file: Path containing the .format file
        data_files: Directory containing all the data files to be loaded
        uri: neo4j database URL
        user: neo4j database username
        password: neo4j database password
    """
    n_headers, file_name_pattern, nodes_and_edges, global_vals, columns, node_primary_key, is_matrix = parse_format(format_file)

    if is_matrix:
        raise ValueError('Format file provided not a tabular one!')

    print('=====Loading formats=====')
    print('Headers:', n_headers)
    print('')
    print('File name:', file_name_pattern)
    print('')
    print('Nodes and edges:')
    for node in nodes_and_edges:
        print(node, nodes_and_edges[node])
    print('')
    print('Global vars in graph:', global_vals)
    print('')
    print('Column formats:')
    for i, column in enumerate(columns):
        print('Column', i, ':', column)
    print('')

    print('=====Loading data files=====')
    for data_file in data_files:
        print('Loading:', data_file)
        file_name = os.path.split(data_file)[-1]
        global_variables = match_re_global(file_name_pattern, global_vals, file_name)
        print('Global vars:', global_variables)
        # ('C', 'tissue', 'Adipose_Subcutaneous', 'str') -> edge C, attribute: tissue, value: Adi..., type: str

        nodes_and_edges_for_this_file = deepcopy(nodes_and_edges) #could be modified to improve efficiency
        for (n, attr, value, _type) in global_variables:
            nodes_and_edges_for_this_file[n][attr] = (value, _type)

        f = gzip.open(data_file) if data_file.endswith('.gz') else open(data_file)

        # skip the headers
        for _ in range(n_headers):
            next(f)

        cnt = 0
        for line in f:
            cnt += 1
            if cnt % 1 == 0:
                print(' ...Loading line:', cnt)

            if data_file.endswith('.gz'):  # the gzip files also need to be decoded
                line = line.decode('utf-8')

            lst = line.split()
            nodes_and_edges_for_this_line = deepcopy(nodes_and_edges_for_this_file) #could be modified to improve efficiency

            for s, pattern in zip(lst, columns):
                if pattern is not None:
                    nodes_and_edges_for_this_line = match_re_nodes(pattern, s, nodes_and_edges_for_this_line)
            for n in nodes_and_edges_for_this_line:
                print(n, nodes_and_edges_for_this_line[n])
            print('')

            # Next: add them to the KG
            importNeo(uri, user, password, nodes_and_edges_for_this_line, node_primary_key)


def load_matrix_data(format_file, data_files, uri, user, password):
    """
    Load matrix data

    Args:
        format_file: Path containing the .format file
        data_files: Directory containing all the data files to be loaded
        uri: neo4j database URL
        user: neo4j database username
        password: neo4j database password
    """
    n_headers, file_name_pattern, nodes_and_edges, global_vals, columns, node_primary_key, is_matrix = parse_format(format_file)

    if not is_matrix:
        raise ValueError('Format file provided not a matrix one!')

    print('=====Loading formats=====')
    print('Headers:', n_headers)
    print('')
    print('File name:', file_name_pattern)
    print('')
    print('Nodes and edges:')
    for node in nodes_and_edges:
        print(node, nodes_and_edges[node])
    print('')
    print('Global vars in graph:', global_vals)
    print('')
    print('Column formats:')
    for i, column in enumerate(columns):
        print('Column', i, ':', column)
    print('')

    print('=====Loading data files=====')
    for data_file in data_files:
        print('Loading:', data_file)
        file_name = os.path.split(data_file)[-1]
        global_variables = match_re_global(file_name_pattern, global_vals, file_name)
        print('Global vars:', global_variables)
        # ('C', 'tissue', 'Adipose_Subcutaneous', 'str') -> edge C, attribute: tissue, value: Adi..., type: str

        nodes_and_edges_for_this_file = deepcopy(nodes_and_edges)  # could be modified to improve efficiency
        for (n, attr, value, _type) in global_variables:
            nodes_and_edges_for_this_file[n][attr] = (value, _type)

        f = gzip.open(data_file) if data_file.endswith('.gz') else open(data_file)

        # skip the headers
        for _ in range(n_headers):
            next(f)

        cnt = 0
        loc = len(columns) - 1
        B_col_attrs = []
        for line in f:
            cnt += 1
            if cnt % 1 == 0:
                print(' ...Loading line:', cnt)

            if data_file.endswith('.gz'):  # the gzip files also need to be decoded
                line = line.decode('utf-8')

            if cnt == 1:
                # interpret the first colname row
                temp = line.split('\t')
                if " " in temp[loc]:
                    B_col_attrs = [convert_attribute(col) for col in temp[loc:]]
                else:
                    B_col_attrs = temp[loc:]
                continue

            lst = line.split()

            # save variables from cnt == 2
            if cnt == 2:
                nodes_and_edges_for_this_line = deepcopy(
                    nodes_and_edges_for_this_file)  # could be modified to improve efficiency

                # make columns and lst the same size
                diff = len(lst) - len(columns)
                temp = columns[-1]
                for i in range(diff):
                    columns.append(temp)

                # enlarge the size of nodes_and_edges_for_this_line to the size of B_nodes
                nodes_and_edges_for_this_line = [
                    match_re_nodes(columns[-1], B_col_attrs[0], nodes_and_edges_for_this_line)]
                for i in range(len(columns)):
                    if i <= loc:
                        continue
                    nodes_and_edges_for_this_line.append(deepcopy(nodes_and_edges_for_this_line[0]))
                    nodes_and_edges_for_this_line[-1] = match_re_nodes(columns[-1], B_col_attrs[i - loc], nodes_and_edges_for_this_line[-1])

                # save the property name of edge C
                edge_prop = ""
                for attr, x in nodes_and_edges_for_this_line[0]['C'].items():
                    if isinstance(x, tuple) and x[0] == 'x':
                        edge_prop = attr

            for i, (s, pattern) in enumerate(zip(lst, columns)):
                if i < loc and pattern is not None:
                    # read in the A attribute first
                    for node_and_edge in nodes_and_edges_for_this_line:
                        node_and_edge = match_re_nodes(pattern, s, node_and_edge)
                else:
                    # for now edge properties aren't allowed to have a patten (TBD)
                    # this line specifically adds the edge property to nodes_and_edges_for_this_line
                    x, type = nodes_and_edges_for_this_line[i - loc]['C'][edge_prop]
                    res = convert(type)(s)
                    nodes_and_edges_for_this_line[i - loc]['C'][edge_prop] = (res, type)

                    # Next: add them to the KG
                    importNeo(uri, user, password, nodes_and_edges_for_this_line[i - loc], node_primary_key)


if __name__ == '__main__':


    load_tabular_data(format_file='GTEx.format',
                      data_files=['Adipose_Subcutaneous.v8.signif_variant_gene_pairs.txt.gz'],
                      uri="bolt://localhost:7687",
                      user="neo4j",
                      password="4DN")
    '''

    load_matrix_data(format_file='GTEx_matrix.format',
                     data_files=['../GTEx/GTEx_Analysis_2017-06-05_v8_RNASeQCv1.1.9_gene_median_tpm.gct'],
                     uri="bolt://localhost:7687",
                     user="neo4j",
                     password="4DN")
    '''

