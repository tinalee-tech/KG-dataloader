import os
import gzip
from copy import deepcopy
from parse_format import parse_format
from utils import *
from py2neo import Graph
import sys
import getopt
import time
import csv
import pandas as pd

MAX_TABLE_ROW = 500000


def create_index(node_primary_key, n, label):
    cyp = "CREATE INDEX IF NOT EXISTS FOR (n:" + label + ") ON ("
    for attr in node_primary_key[n]:
        cyp += "n." + attr + ", "
    cyp = cyp[:-2] + ")"
    return cyp


def exclude_null_primary_key_rows(node_primary_key, n_list):
    cyp = "WITH row WHERE "
    for n in n_list:
        for attr in node_primary_key[n]:
            colname = str(n) + "_" + str(attr)
            cyp += "row."+colname+" <> '' AND "
    cyp = cyp[:-4] + "\n"
    return cyp


def importNeo(format_file, data_files, uri, user, password):
    """
    Import data into neo4j database

    Args:
        uri: Neo4j uri
        user: Neo4j user name
        password: database password
        format_file: Path containing the .format file
        data_files: Directory containing all the data files to be loaded
    """
    graph = Graph(uri, user=user, password=password)

    n_headers, file_name_pattern, nodes_and_edges, global_vals, columns, node_primary_key, is_matrix, chr_chain_info, \
    delimiter = parse_format(format_file)

    if os.path.isfile(data_files):
        path = [data_files]
    else:
        path = os.listdir(data_files)

    first_file = True
    print("=====Importing data files into database=====")
    nodes_and_edges_for_this_file = deepcopy(nodes_and_edges)  # could be modified to improve efficiency
    csv_chr_chain_colnames = {}
    for n in chr_chain_info.keys():
        csv_chr_chain_colnames[n] = ["chr", "resolution", "start_loc"]
        for attr in node_primary_key[n]:
            csv_chr_chain_colnames[n].append(str(n) + "_" + str(attr))

    for data_file in path:
        file_name = os.path.split(data_file)[-1]
        clean_file_name = file_name.split(".")[0]
        neo4j_file_name = clean_file_name + ".csv"
        print(f"Importing file {neo4j_file_name}...")

        if first_file:
            # get all the information of the written csv files
            try:
                global_variables = match_re_global(file_name_pattern, global_vals, file_name)
            except AttributeError:
                # not the intended file
                continue
            for (n, attr, value, _type) in global_variables:
                nodes_and_edges_for_this_file[n][attr] = (value, _type)
            first_file = False

        # load nodes
        print(f"Starting to load all nodes in {neo4j_file_name}...")

        for n, attrs in nodes_and_edges_for_this_file.items():
            if nodes_and_edges_for_this_file[n]['node_or_edge'] == 'edge':
                continue

            print(f"Starting to load node {n} in {neo4j_file_name}...")
            # create index if not exist
            cyp = create_index(node_primary_key, n, nodes_and_edges_for_this_file[n]['label'])
            graph.run(cyp)

            set = ""
            cypher = "USING PERIODIC COMMIT 1000\n"
            cypher += "LOAD CSV WITH HEADERS FROM 'file:///" + neo4j_file_name + "' AS row\n"
            cypher += exclude_null_primary_key_rows(node_primary_key, [n])
            cypher += "MERGE (n:"+nodes_and_edges_for_this_file[n]['label']+" {"
            for attr in attrs.keys():
                if attr == "node_or_edge" or attr == "label":
                    continue
                colname = str(n) + "_" + str(attr)
                if attr in node_primary_key[n]:
                    cypher += attr + ": "+neo4j_type_convert(nodes_and_edges_for_this_file[n][attr][1])+"(row."+colname+"), "
                else:
                    set += "SET n."+attr+" = CASE row."+colname+" WHEN '' THEN null ELSE "+neo4j_type_convert(nodes_and_edges_for_this_file[n][attr][1])+"(row."+colname+") END \n"
            set += "SET n.add_time = datetime()\n"

            cypher = cypher[:-2] + " })\n"
            cypher += set
            cypher += "RETURN count(n)"
            graph.run(cypher=cypher)

        print(f"Finished loading all nodes in {neo4j_file_name}")

        # load edges
        print(f"Starting to load all edges in {neo4j_file_name}...")
        for n, attrs in nodes_and_edges_for_this_file.items():
            if nodes_and_edges_for_this_file[n]['node_or_edge'] == 'node':
                continue
            print(f"Starting to load edge {n} in {neo4j_file_name}...")
            A = nodes_and_edges_for_this_file[n]['from']
            B = nodes_and_edges_for_this_file[n]['to']

            cypher = "USING PERIODIC COMMIT 1000\n"
            cypher += "LOAD CSV WITH HEADERS FROM 'file:///" + neo4j_file_name + "' AS row\n"
            cypher += exclude_null_primary_key_rows(node_primary_key, [A, B])
            cypher += "MATCH (n:" + nodes_and_edges_for_this_file[A]['label'] + " {"
            for attr in nodes_and_edges_for_this_file[A].keys():
                if attr == "node_or_edge" or attr == "label":
                    continue
                colname = str(A) + "_" + str(attr)
                if attr in node_primary_key[A]:
                    cypher += attr + ": " + neo4j_type_convert(
                        nodes_and_edges_for_this_file[A][attr][1]) + "(row." + colname + "), "
            cypher = cypher[:-2] + " })\n"

            cypher += "MATCH (e:" + nodes_and_edges_for_this_file[B]['label'] + " {"
            for attr in nodes_and_edges_for_this_file[B].keys():
                if attr == "node_or_edge" or attr == "label":
                    continue
                colname = str(B) + "_" + str(attr)
                if attr in node_primary_key[B]:
                    cypher += attr + ": " + neo4j_type_convert(
                        nodes_and_edges_for_this_file[B][attr][1]) + "(row." + colname + "), "
            cypher = cypher[:-2] + " })\n"

            set = ""
            for attr in nodes_and_edges_for_this_file[n].keys():
                if attr != 'node_or_edge' and attr != 'label' and attr != 'from' and attr != 'to':
                    colname = str(n) + "_" + str(attr)
                    set += "SET r."+attr+" = CASE row."+colname+" WHEN '' THEN null ELSE "+neo4j_type_convert(nodes_and_edges_for_this_file[n][attr][1])+"(row."+colname+") END \n"

            cypher += "MERGE (n) - [r:"+nodes_and_edges_for_this_file[n]['label']+"] -> (e)\n"
            cypher += set
            cypher += "SET r.add_time = datetime()\n"
            graph.run(cypher=cypher)
        print(f"Finished loading all edges in {neo4j_file_name}...")

        # load chr_chain
        print(f"Starting to load all chr_chain edges...")
        for n in chr_chain_info.keys():
            print(f"Starting to load chr_chain edge in {clean_file_name}.chr_chain_for_{n}.csv...")

            cypher = "USING PERIODIC COMMIT 1000\n"
            cypher += "LOAD CSV WITH HEADERS FROM 'file:///" + clean_file_name + ".chr_chain_for_"+n+".csv" "' AS row\n"
            cypher += exclude_null_primary_key_rows(node_primary_key, [n])
            cypher += "MATCH (n:" + nodes_and_edges_for_this_file[n]['label'] + " {"
            for attr in nodes_and_edges_for_this_file[n].keys():
                if attr == "node_or_edge" or attr == "label":
                    continue
                if attr in node_primary_key[n]:
                    colname = str(n) + "_" + str(attr)
                    cypher += attr + ": " + neo4j_type_convert(
                        nodes_and_edges_for_this_file[n][attr][1]) + "(row." + colname + "), "
            cypher = cypher[:-2] + " })\n"

            cypher += "MATCH (e:chr_chain {"
            for attr in ["chr", "resolution", "start_loc"]:
                colname = attr
                if attr == 'resolution':
                    cypher += attr + ": toInteger(row." + colname + "), "
                elif attr == 'chr':
                    cypher += attr + ": toString(row." + colname + "), "
                else:
                    cypher += attr + ": toInteger(row." + colname + "), "
            cypher = cypher[:-2] + " })\n"

            cypher += "MERGE (n) - [r:locate_on_chain] -> (e)\n"
            cypher += "SET r.add_time = datetime()\n"
            graph.run(cypher=cypher)

        print(f"Finished loading all chr_chain edges")


def make_csv_tabular(format_file, data_files, neo4j_home):
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

    # find csv column names
    csv_colnames = []
    csv_chr_chain_colnames = {}
    for n in chr_chain_info.keys():
        csv_chr_chain_colnames[n] = ["chr", "resolution", "start_loc"]
        for attr in node_primary_key[n]:
            csv_chr_chain_colnames[n].append(str(n)+"_"+str(attr))

    for data_file in path:
        table = []
        table_chr_chain = {}
        init_commit_chr_chain_csv = {}
        for node in csv_chr_chain_colnames.keys():
            table_chr_chain[node] = []
            init_commit_chr_chain_csv[node] = False
        init_commit = False

        print('Loading:', data_file)
        file_name = os.path.split(data_file)[-1]
        try:
            global_variables = match_re_global(file_name_pattern, global_vals, file_name)
        except AttributeError:
            # not the intended file
            print(f"{data_file} is not included by file_name in format file. Looking for next file...")
            continue
        print('Global vars:', global_variables)

        nodes_and_edges_for_this_file = deepcopy(nodes_and_edges)  # could be modified to improve efficiency
        for (n, attr, value, _type) in global_variables:
            nodes_and_edges_for_this_file[n][attr] = (value, _type)

        # find the csv_colnames, same for all data files
        if len(csv_colnames) == 0:
            for n, attrs in nodes_and_edges_for_this_file.items():
                for attr in attrs.keys():
                    if attr != "node_or_edge" and attr != "label" and attr != "from" and attr != "to":
                        csv_colnames.append(str(n)+"_"+str(attr))

        if isfile == False:
            if data_files[-1] == "/":
                f = gzip.open(data_files + data_file) if data_file.endswith('.gz') else open(data_files + data_file)
            else:
                f = gzip.open(data_files + "/" + data_file) if data_file.endswith('.gz') else open(data_files + "/" + data_file)
        else:
            f = gzip.open(data_file) if data_file.endswith('.gz') else open(data_file)

        # skip the headers
        for _ in range(n_headers):
            next(f)

        cnt = 0
        for line in f:
            table_row = []
            cnt += 1
            if cnt % 1 == 0:
                print(' ...Loading line:', cnt)

            if data_file.endswith('.gz'):  # the gzip files also need to be decoded
                line = line.decode('utf-8').strip('\x00')
            if len(line.strip()) == 0:
                continue

            lst = line.strip('\n').split(delimiter)
            if len(lst) != len(columns):
                raise ValueError(f'In .format file, line_format section should have all the columns in data file')
            nodes_and_edges_for_this_line = deepcopy(
                nodes_and_edges_for_this_file)  # could be modified to improve efficiency

            for s, pattern in zip(lst, columns):
                if pattern is not None:
                    nodes_and_edges_for_this_line = match_re_nodes(pattern, s, nodes_and_edges_for_this_line)
            if len(chr_chain_info) != 0:
                # some nodes need to connect to chr_chain
                for colname in csv_colnames:
                    node, attr = colname.split("_", maxsplit=1)
                    table_row.append(nodes_and_edges_for_this_line[node][attr][0])

                for node in csv_chr_chain_colnames.keys():
                    for i in range(len(chr_chain_info[node])):
                        start_loc = nodes_and_edges_for_this_line[node][chr_chain_info[node][i]["start_loc"]][0]
                        end_loc = nodes_and_edges_for_this_line[node][chr_chain_info[node][i]["end_loc"]][0]
                        resolution = chr_chain_info[node][i]["resolution"]
                        chr = nodes_and_edges_for_this_line[node][chr_chain_info[node][i]["chr"]][0]
                        if chr == "" or start_loc == "" or end_loc == "":
                            continue
                        all_start_pos = find_all_start_locs(start_loc, end_loc, resolution)
                        for target_node_start_pos in all_start_pos:
                            table_row_chr_chain = []
                            for attr in csv_chr_chain_colnames[node]:
                                if attr == "chr":
                                    table_row_chr_chain.append(chr)
                                elif attr == "start_loc":
                                    table_row_chr_chain.append(target_node_start_pos)
                                elif attr == "resolution":
                                    table_row_chr_chain.append(resolution)
                                else:
                                    n, attr = attr.split("_", maxsplit=1)
                                    assert n == node
                                    table_row_chr_chain.append(nodes_and_edges_for_this_line[node][attr][0])
                            assert len(table_row_chr_chain) == len(csv_chr_chain_colnames[node])
                            table_chr_chain[node].append(table_row_chr_chain)

                    # for memory's sake, periodic write to output file
                    if len(table_chr_chain[node]) > MAX_TABLE_ROW:
                        df = pd.DataFrame(table_chr_chain[node], columns=csv_chr_chain_colnames[node])
                        clean_file_name = file_name.split(".")[0]
                        if init_commit_chr_chain_csv[node] == False:
                            df.to_csv(neo4j_home + clean_file_name + ".chr_chain_for_"+node+".csv", index=False, mode='w')
                            init_commit_chr_chain_csv[node] = True
                        else:
                            df.to_csv(neo4j_home + clean_file_name + ".chr_chain_for_"+node+".csv", index=False, header=False, mode='a+')
                        table_chr_chain[node] = []

            else:
                for colname in csv_colnames:
                    node, attr = colname.split("_", maxsplit=1)
                    table_row.append(nodes_and_edges_for_this_line[node][attr][0])

            assert len(table_row) == len(csv_colnames)
            table.append(table_row)
            print('')

            # for memory's sake, periodic write to output file
            if len(table) > MAX_TABLE_ROW:
                df = pd.DataFrame(table, columns=csv_colnames)
                clean_file_name = file_name.split(".")[0]
                if init_commit == False:
                    df.to_csv(neo4j_home + clean_file_name + ".csv", index=False, mode='w')
                    init_commit = True
                else:
                    df.to_csv(neo4j_home + clean_file_name + ".csv", index=False, header=False, mode='a+')
                table = []

        df = pd.DataFrame(table, columns=csv_colnames)
        clean_file_name = file_name.split(".")[0]
        if init_commit == False:
            df.to_csv(neo4j_home + clean_file_name + ".csv", index=False, mode='w')
        else:
            df.to_csv(neo4j_home + clean_file_name + ".csv", index=False, header=False, mode='a+')
        print(f"Successfully written to file {neo4j_home}{clean_file_name}.csv")

        for node in table_chr_chain.keys():
            df = pd.DataFrame(table_chr_chain[node], columns=csv_chr_chain_colnames[node])
            clean_file_name = file_name.split(".")[0]
            if init_commit_chr_chain_csv[node] == False:
                df.to_csv(neo4j_home + clean_file_name + ".chr_chain_for_" + node + ".csv", index=False, mode='w')
            else:
                df.to_csv(neo4j_home + clean_file_name + ".chr_chain_for_" + node + ".csv", index=False, header=False,
                          mode='a+')
            print(f"Successfully written to file {neo4j_home}{clean_file_name}.chr_chain_for_{node}.csv")

        f.close()


def make_csv_matrix(format_file, data_files, neo4j_home):
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

    if is_matrix == False:
        print("Format file's format_type is not matrix")
        raise Exception

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

    # find csv column names
    csv_colnames = []
    csv_chr_chain_colnames = {}
    for n in chr_chain_info.keys():
        csv_chr_chain_colnames[n] = ["chr", "resolution", "start_loc"]
        for attr in node_primary_key[n]:
            csv_chr_chain_colnames[n].append(str(n)+"_"+str(attr))

    for data_file in path:
        table = []
        table_chr_chain = {}
        init_commit_chr_chain_csv = {}
        for node in csv_chr_chain_colnames.keys():
            table_chr_chain[node] = []
            init_commit_chr_chain_csv[node] = False
        init_commit = False

        print('Loading:', data_file)
        file_name = os.path.split(data_file)[-1]
        try:
            global_variables = match_re_global(file_name_pattern, global_vals, file_name)
        except AttributeError:
            # not the intended file
            print(f"{data_file} is not included by file_name in format file. Looking for next file...")
            continue
        print('Global vars:', global_variables)

        nodes_and_edges_for_this_file = deepcopy(nodes_and_edges)  # could be modified to improve efficiency
        for (n, attr, value, _type) in global_variables:
            nodes_and_edges_for_this_file[n][attr] = (value, _type)

        # find the csv_colnames, same for all data files
        if len(csv_colnames) == 0:
            for n, attrs in nodes_and_edges_for_this_file.items():
                for attr in attrs.keys():
                    if attr != "node_or_edge" and attr != "label" and attr != "from" and attr != "to":
                        csv_colnames.append(str(n)+"_"+str(attr))

        if isfile == False:
            if data_files[-1] == "/":
                f = gzip.open(data_files + data_file) if data_file.endswith('.gz') else open(data_files + data_file)
            else:
                f = gzip.open(data_files + "/" + data_file) if data_file.endswith('.gz') else open(data_files + "/" + data_file)
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
                line = line.decode('utf-8').strip('\x00')
            if len(line.strip()) == 0:
                continue

            if cnt == 1:
                # interpret the first colname row
                lst = line.strip('\n').split(delimiter)
                B_col_attrs = [convert_attribute(col) for col in lst[loc:]]
                continue

            lst = line.strip('\n').split(delimiter)

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
                    nodes_and_edges_for_this_line[-1] = match_re_nodes(columns[-1], B_col_attrs[i - loc],
                                                                       nodes_and_edges_for_this_line[-1])

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
                elif i >= loc:
                    # for now edge properties aren't allowed to have a patten (TBD)
                    # this line specifically adds the edge property to nodes_and_edges_for_this_line
                    x, type = nodes_and_edges_for_this_line[i - loc]['C'][edge_prop]
                    res = convert(type)(s)
                    nodes_and_edges_for_this_line[i - loc]['C'][edge_prop] = (res, type)

            if len(chr_chain_info) != 0:
                '''
                # some nodes need to connect to chr_chain
                for colname in csv_colnames:
                    node, attr = colname.split("_", maxsplit=1)
                    table_row.append(nodes_and_edges_for_this_line[node][attr][0])

                for node in csv_chr_chain_colnames.keys():
                    for i in range(len(chr_chain_info[node])):
                        start_loc = nodes_and_edges_for_this_line[node][chr_chain_info[node][i]["start_loc"]][0]
                        end_loc = nodes_and_edges_for_this_line[node][chr_chain_info[node][i]["end_loc"]][0]
                        resolution = chr_chain_info[node][i]["resolution"]
                        chr = nodes_and_edges_for_this_line[node][chr_chain_info[node][i]["chr"]][0]
                        if chr == "" or start_loc == "" or end_loc == "":
                            continue
                        all_start_pos = find_all_start_locs(start_loc, end_loc, resolution)
                        for target_node_start_pos in all_start_pos:
                            table_row_chr_chain = []
                            for attr in csv_chr_chain_colnames[node]:
                                if attr == "chr":
                                    table_row_chr_chain.append(chr)
                                elif attr == "start_loc":
                                    table_row_chr_chain.append(target_node_start_pos)
                                elif attr == "resolution":
                                    table_row_chr_chain.append(resolution)
                                else:
                                    n, attr = attr.split("_", maxsplit=1)
                                    assert n == node
                                    table_row_chr_chain.append(nodes_and_edges_for_this_line[node][attr][0])
                            assert len(table_row_chr_chain) == len(csv_chr_chain_colnames[node])
                            table_chr_chain[node].append(table_row_chr_chain)

                    # for memory's sake, periodic write to output file
                    if len(table_chr_chain[node]) > MAX_TABLE_ROW:
                        df = pd.DataFrame(table_chr_chain[node], columns=csv_chr_chain_colnames[node])
                        clean_file_name = file_name.split(".")[0]
                        if init_commit_chr_chain_csv[node] == False:
                            df.to_csv(neo4j_home + clean_file_name + ".chr_chain_for_"+node+".csv", index=False, mode='w')
                            init_commit_chr_chain_csv[node] = True
                        else:
                            df.to_csv(neo4j_home + clean_file_name + ".chr_chain_for_"+node+".csv", index=False, header=False, mode='a+')
                        table_chr_chain[node] = []
                '''
                pass

            else:
                for node_and_edge in nodes_and_edges_for_this_line:
                    table_row = []
                    for colname in csv_colnames:
                        node, attr = colname.split("_", maxsplit=1)
                        table_row.append(node_and_edge[node][attr][0])
                    assert len(table_row) == len(csv_colnames)
                    table.append(table_row)

            print('')

            # for memory's sake, periodic write to output file
            if len(table) > MAX_TABLE_ROW:
                df = pd.DataFrame(table, columns=csv_colnames)
                clean_file_name = file_name.split(".")[0]
                if init_commit == False:
                    df.to_csv(neo4j_home + clean_file_name + ".csv", index=False, mode='w')
                    init_commit = True
                else:
                    df.to_csv(neo4j_home + clean_file_name + ".csv", index=False, header=False, mode='a+')
                table = []

        df = pd.DataFrame(table, columns=csv_colnames)
        clean_file_name = file_name.split(".")[0]
        if init_commit == False:
            df.to_csv(neo4j_home + clean_file_name + ".csv", index=False, mode='w')
        else:
            df.to_csv(neo4j_home + clean_file_name + ".csv", index=False, header=False, mode='a+')
        print(f"Successfully written to file {neo4j_home}{clean_file_name}.csv")

        for node in table_chr_chain.keys():
            df = pd.DataFrame(table_chr_chain[node], columns=csv_chr_chain_colnames[node])
            clean_file_name = file_name.split(".")[0]
            if init_commit_chr_chain_csv[node] == False:
                df.to_csv(neo4j_home + clean_file_name + ".chr_chain_for_" + node + ".csv", index=False, mode='w')
            else:
                df.to_csv(neo4j_home + clean_file_name + ".chr_chain_for_" + node + ".csv", index=False, header=False,
                          mode='a+')
            print(f"Successfully written to file {neo4j_home}{clean_file_name}.chr_chain_for_{node}.csv")

        f.close()



if __name__ == "__main__":
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
                print(
                    "\t\t<format file>:  The required .format file for dataloader is specially defined.  Please refer to Sec-tion.4.1 for the detailed description.")
                print(
                    "\t\t<data file>: The directory including all the data files to be loaded into the knowledge graph.")
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

    # print("username: ", end="")
    # user = input()
    # print("password: ", end="")
    # password = getpass.getpass()
    user = 'neo4j'
    password = 'password'

    # neo4j_home = '/opt/neo4j/neo4j-community-4.2.4/import/'
    # neo4j_home = "~/Library/Application Support/Neo4j Desktop/Application/relate-data/dbmss/dbms-3e52da25-7ca5-4dc9-992b-068819174145/import/"
    # neo4j_home = "./"
    neo4j_home = "./clean_csv/"

    make_csv_tabular(args[0], args[1], neo4j_home)
    # make_csv_matrix(args[0], args[1], neo4j_home)
    # importNeo(args[0], args[1], uri, user, password)

    time_end = time.time()
    print('time cost: ', time_end - time_start, ' s')