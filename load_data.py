import os
import gzip
from copy import deepcopy
from parse_format import parse_format
from utils import match_re_global, match_re_nodes, convert_attribute, convert, find_start_loc
from py2neo import Graph
import sys
import getopt
import time


def importNeo(uri, user, password, nodes_and_edges, node_primary_key, chr_chain_info, node_created):
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
    cyp_node_name = {}
    total_cypher = ""
    temp = ""
    for node in node_primary_key.keys(): #
        cypher = "MERGE (" + node.lower() + ":" + nodes_and_edges[node]['label'] + " {"
        onset = node.lower() + ".add_time = datetime(), "

        # merge on primary keys
        for attr in node_primary_key[node]:
            if nodes_and_edges[node][attr][1] == 'str':
                cypher = cypher + attr + ": \"" + nodes_and_edges[node][attr][0] + "\", "
            else:
                cypher = cypher + attr + ": " + str(nodes_and_edges[node][attr][0]) + ", "

        # check if the node has been created
        if cypher not in node_created.keys():
            node_created[cypher] = True
            # on merge renew or create other attributes
            for attr in nodes_and_edges[node].keys():
                if attr != 'node_or_edge' and attr != 'label' and attr not in node_primary_key[node]:
                    if nodes_and_edges[node][attr][1] == 'str':
                        onset = onset + node.lower() + "." + attr + "= \"" + nodes_and_edges[node][attr][0] + "\", "
                    else:
                        onset = onset + node.lower() + "." + attr + "= " + str(nodes_and_edges[node][attr][0]) + ", "

            cypher = cypher[:-2] + "} ) ON CREATE SET " + onset[:-2] + " ON MATCH SET " + onset[:-2] + " "

            total_cypher = total_cypher + cypher
            cyp_node_name[node] = node.lower()

            # merge the node's edge on chr_chain if applicable
            if node in chr_chain_info.keys():
                resolution = chr_chain_info[node]["resolution"]
                temp += node.lower() + ", "
                pos = find_start_loc(nodes_and_edges[node][chr_chain_info[node]["pos"]][0], resolution)
                # Optional matches ensures if chr_chain can't be found, no error will be thrown, other merges still run
                cypher = "OPTIONAL MATCH (n:chr_chain) WHERE n.chr = \"" + \
                        nodes_and_edges[node][chr_chain_info[node]["chr"]][0] + "\" AND " + \
                        "n.resolution = " + chr_chain_info[node]["resolution"] + " AND " + "n.start_loc = " + str(
                        pos) + " FOREACH (node IN [x in [n] WHERE x is not NULL] |" + \
                         " MERGE (" + node.lower() + ") - [:locate_in {type: \"chr_chain\"}] -> (node) ) "
                # TODO: If all chr_chain have been added, above line change to following to increase efficiency
                # cypher = "MATCH (n:chr_chain) WHERE n.chr = \"" + \
                #         nodes_and_edges[node][chr_chain_info[node]["chr"]][0] + "\" AND " + \
                #         "n.resolution = " + chr_chain_info[node]["resolution"] + " AND " + "n.start_loc = " + str(
                #         pos) + " MERGE (" + node.lower() + ") - [:locate_in {type: \"chr_chain\"}] -> (n) "
                total_cypher += "WITH " + temp[:-2] + " " + cypher

    # merge edge
    for edge in nodes_and_edges.keys():
        if nodes_and_edges[edge]['node_or_edge'] == 'node':
            continue
        node1 = nodes_and_edges[edge]['from']
        node2 = nodes_and_edges[edge]['to']

        if node1 not in cyp_node_name.keys() and node2 not in cyp_node_name.keys():
            cypher = "MATCH (" + node1.lower() + ":" + nodes_and_edges[node1]['label'] + "), (" + node2.lower() + ":" + nodes_and_edges[node2]['label'] + ") WHERE "
            for node, n in zip([node1, node2], [node1.lower() + '.', node2.lower() + '.']):
                for attr in node_primary_key[node]:
                    if nodes_and_edges[node][attr][1] == 'str':
                        cypher = cypher + n + attr + " = \"" + nodes_and_edges[node][attr][0] + "\" AND "
                    else:
                        cypher = cypher + n + attr + " = " + str(nodes_and_edges[node][attr][0]) + " AND "
            total_cypher = total_cypher + "WITH " + temp[:-2] + " " + cypher[:-4]
        elif node1 not in cyp_node_name.keys():
            cypher = "MATCH (" + node1.lower() + ":" + nodes_and_edges[node1]['label'] + ") WHERE "
            for node, n in zip([node1], [node1.lower() + '.']):
                for attr in node_primary_key[node]:
                    if nodes_and_edges[node][attr][1] == 'str':
                        cypher = cypher + n + attr + " = \"" + nodes_and_edges[node][attr][0] + "\" AND "
                    else:
                        cypher = cypher + n + attr + " = " + str(nodes_and_edges[node][attr][0]) + " AND "
            total_cypher = total_cypher + "WITH " + temp[:-2] + " " + cypher[:-4]
        elif node2 not in cyp_node_name.keys():
            cypher = "MATCH (" + node2.lower() + ":" + nodes_and_edges[node2]['label'] + ") WHERE "
            for node, n in zip([node2], [node2.lower() + '.']):
                for attr in node_primary_key[node]:
                    if nodes_and_edges[node][attr][1] == 'str':
                        cypher = cypher + n + attr + " = \"" + nodes_and_edges[node][attr][0] + "\" AND "
                    else:
                        cypher = cypher + n + attr + " = " + str(nodes_and_edges[node][attr][0]) + " AND "
            total_cypher = total_cypher + "WITH " + temp[:-2] + " " + cypher[:-4]

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
            cypher = "MERGE (" + node1.lower() + ") - [:" + nodes_and_edges[edge]['label'] + "] -> (" + node2.lower() + ") "
        else:
            cypher = "MERGE (" + node1.lower() + ") - [:" + nodes_and_edges[edge]['label'] + edgepropstr + "] -> (" + node2.lower() + ") "

        total_cypher = total_cypher + cypher

    graph.run(cypher=total_cypher)


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
    n_headers, file_name_pattern, nodes_and_edges, global_vals, columns, node_primary_key, is_matrix, chr_chain_info, \
    delimiter = parse_format(format_file)

    if is_matrix:
        raise ValueError(f'Format file provided not a tabular one!')

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
    isfile = False
    if os.path.isfile(data_files):
        isfile = True
        path = [data_files]
    else:
        path = os.listdir(data_files)

    node_created = {}
    for data_file in path:
        print('Loading:', data_file)
        file_name = os.path.split(data_file)[-1]
        try:
            global_variables = match_re_global(file_name_pattern, global_vals, file_name)
        except AttributeError:
            # not the intended file
            continue
        print('Global vars:', global_variables)
        # ('C', 'tissue', 'Adipose_Subcutaneous', 'str') -> edge C, attribute: tissue, value: Adi..., type: str

        nodes_and_edges_for_this_file = deepcopy(nodes_and_edges) #could be modified to improve efficiency
        for (n, attr, value, _type) in global_variables:
            nodes_and_edges_for_this_file[n][attr] = (value, _type)

        if isfile == False:
            f = gzip.open(data_files + "/" +  data_file) if data_file.endswith('.gz') else open(data_file)
        else:
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

            lst = line.strip().split(delimiter)
            nodes_and_edges_for_this_line = deepcopy(nodes_and_edges_for_this_file) #could be modified to improve efficiency

            for s, pattern in zip(lst, columns):
                if pattern is not None:
                    nodes_and_edges_for_this_line = match_re_nodes(pattern, s, nodes_and_edges_for_this_line)
            for n in nodes_and_edges_for_this_line:
                print(n, nodes_and_edges_for_this_line[n])
            print('')

            # Next: add them to the KG
            importNeo(uri, user, password, nodes_and_edges_for_this_line, node_primary_key, chr_chain_info, node_created)


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
    n_headers, file_name_pattern, nodes_and_edges, global_vals, columns, node_primary_key, is_matrix, chr_chain_info = parse_format(format_file)

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
    isfile = False
    if os.path.isfile(data_files):
        isfile = True
        path = [data_files]
    else:
        path = os.listdir(data_files)

    node_created = {}
    for data_file in path:
        print('Loading:', data_file)
        file_name = os.path.split(data_file)[-1]
        try:
            global_variables = match_re_global(file_name_pattern, global_vals, file_name)
        except AttributeError:
            # not the intended file
            continue
        print('Global vars:', global_variables)

        nodes_and_edges_for_this_file = deepcopy(nodes_and_edges)  # could be modified to improve efficiency
        for (n, attr, value, _type) in global_variables:
            nodes_and_edges_for_this_file[n][attr] = (value, _type)

        if isfile == False:
            f = gzip.open(data_files + "/" + data_file) if data_file.endswith('.gz') else open(data_file)
        else:
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

            lst = line.strip().split('\t')

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
                    importNeo(uri, user, password, nodes_and_edges_for_this_line[i - loc], node_primary_key, chr_chain_info, node_created)


def delete_all(uri, user, password):
    graph = Graph(uri, user=user, password=password)
    graph.delete_all()


if __name__ == '__main__':
    time_start = time.time()

    try:
        my_args = []
        for args in sys.argv[1:]:
            my_str = bytes(args, 'utf-8').decode('unicode_escape')
            my_args = my_args + [my_str]
        options, args = getopt.getopt(my_args, "hi:", ["help", "uri="])
    except getopt.GetoptError:
        print("GetoptError: See -h for help.")
        sys.exit(1)

    try:
        if len(args) < 2 and options[0][0] not in ("-h", "--help"):
            raise Exception()
    except Exception:
        print("GetoptError: must specifiy .format file and data file path in the end. See -h for help.")
        sys.exit(1)

    try:
        data_files = ""
        format_file = ""
        uri = "bolt://localhost:7687"
        for option, value in options:
            if option in ("-h", "--help"):
                print(
                    "Usage: python3 load_data.py [-i URI] <format file> <data files>")
                print("\trequired arguments: ")
                print("\t\t<format file>:  The required .format file for dataloader is specially defined.  Please refer to Sec-tion.4.1 for the detailed description.")
                print("\t\t<data file>: The directory including all the data files to be loaded into the knowledge graph.")
                print("\toptional arguments: ")
                print(
                    "\t\t[-i URI]: The URL of the neo4j knowledge base. Without specification, by default usesbolt://localhost:7687")
                exit(0)
            elif option in ("-i", "--uri"):
                uri = value
            else:
                print("GetoptError: See -h for help.")
                raise Exception
    except Exception:
        sys.exit(1)


    #print("username: ", end="")
    #user = input()
    #print("password: ", end="")
    #password = getpass.getpass()
    user = 'neo4j'
    password = '4DN'

    load_tabular_data(format_file=args[0],
                      data_files=args[1],
                      uri=uri,
                      user=user,
                      password=password)

    time_end = time.time()
    print('time cost: ', time_end - time_start, ' s')
    '''
    load_matrix_data(format_file=args[0],
                     data_files=args[1],
                     uri=uri,
                     user=user,
                     password=password)
    
    delete_all(uri, user, password)
    '''