import os
import gzip
from copy import deepcopy
from parse_format import parse_format
from utils import match_re_global, match_re_nodes, convert_attribute, convert
from py2neo import Graph
import sys
import getopt
import time
import math
import os
import getpass

def create_gene_chains(uri, user, password, chrom_size_file):
    graph = Graph(uri, user=user, password=password)
    f = open(chrom_size_file)

    for line in f.readlines():
        chr_num, end = line.split()
        chr_num = chr_num[3:]

        prev_resolution = 50000
        # for resolution in [20000, 10000, 5000, 1000, 200]:
        for resolution in [50000, 20000, 10000, 5000, 1000, 200]:
            num = math.ceil(float(end) / float(resolution))
            end_res = num * resolution
            for i in range(num):
                start_loc = 1 + i * resolution
                end_loc = (i + 1) * resolution

                if resolution != 50000:
                    parent_start_loc = math.floor(float(start_loc) / float(prev_resolution)) * prev_resolution + 1

                if i == 0:
                    cypher = "MERGE (n:chr_chain {resolution: " + str(resolution) + ", chr:\"" + chr_num + "\", "
                    cypher += "start_loc: " + str(start_loc) + ", " + "end_loc: " + str(end_loc) + "}) "
                    if resolution != 50000:
                        cypher += "WITH n AS target "
                        cypher += "MATCH (p:chr_chain {resolution: " + str(prev_resolution) + ", chr: \"" + chr_num + \
                                  "\", start_loc: " + str(parent_start_loc) + \
                                  "}) MERGE (target) - [:lower_resolution] -> (p)"

                    graph.run(cypher=cypher)
                    print(
                        f"process: {i + 1}/{num}, chr_num: {chr_num}, resolution: {resolution}, start_loc: {start_loc}, end_loc: {end_loc}")
                    continue

                cypher = "MATCH (n:chr_chain {resolution: " + str(resolution) + ", chr:\"" + chr_num + "\", "
                cypher += "start_loc: " + str(start_loc - resolution) + ", " + "end_loc: " + str(end_loc - resolution) + "}) "
                cypher += "MERGE (n) - [:next_loc] -> (m:chr_chain {resolution: " + str(resolution) + ", chr:\"" + chr_num + "\", "
                cypher += "start_loc: " + str(start_loc) + ", " + "end_loc: " + str(end_loc) + "}) "

                if resolution != 50000:
                    cypher += "WITH m AS target "
                    cypher += "MATCH (p:chr_chain {resolution: " + str(prev_resolution) + ", chr: \"" + chr_num + \
                              "\", start_loc: " + str(parent_start_loc) + \
                              "}) MERGE (target) - [:lower_resolution] -> (p)"
                print(
                    f"process: {i + 1}/{num}, chr_num: {chr_num}, resolution: {resolution}, start_loc: {start_loc}, end_loc: {end_loc}")
                graph.run(cypher=cypher)

            prev_resolution = resolution

    f.close()

if __name__ == "__main__":
    os.system("ssh -L 7687:localhost:7687 jieliulab.dcmb.med.umich.edu")

    uri = "bolt://localhost:7687"
    user = 'neo4j'
    password = 'password'

    create_gene_chains(uri, user, password, sys.argv[1])
