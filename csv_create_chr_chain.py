from py2neo import Graph
import time
import math
import pandas as pd


def make_csv(gc_dir, is50000, outfile):
    if is50000:
        table = []

        f = open("./hg38.chrom.sizes")
        f_gc = open(gc_dir + "GC_percentage_50000.bed")
        for line in f.readlines():
            chr_num, end = line.split()
            chr_num = chr_num[3:]

            resolution = 50000
            print(f"Writing resoulution:{resolution}, chr:{chr_num} to csv file...")

            num = math.ceil(float(end) / float(resolution))

            for i in range(num):
                start_loc = 0 + i * resolution
                end_loc = (i + 1) * resolution - 1
                next_start_loc = start_loc + resolution
                next_end_loc = end_loc + resolution
                gc_line = f_gc.readline()

                if i == num - 1:
                    end_loc = int(end)
                    next_start_loc = "NaN"
                    next_end_loc = "NaN"
                _chr,_start,_end,gc_percentage = gc_line.split()
                _chr = _chr[3:]
                print(f"chr: {chr_num}, _chr: {_chr}, start_loc: {start_loc}, _start: {_start}, end_loc: {end_loc}, _end: {_end}")
                assert _chr == chr_num
                assert _start == str(start_loc)

                table.append(
                    [chr_num, resolution, start_loc, end_loc, next_start_loc, next_end_loc, gc_percentage])
        f_gc.close()
        f.close()

        df = pd.DataFrame(table, columns=["chr", "resolution", "start_loc", "end_loc", "next_start_loc", "next_end_loc", "GC_percentage"])
        df.to_csv(neo4j_home + outfile, index=False)
        print(f"Finished writing all chr_nums to csv file {outfile}")
        print("Starting to load csv file into database.")

        return
    else:
        prev_resolution = 50000
        for resolution in [10000, 5000, 1000, 200]:
        # for resolution in [10000, 5000, 1000]:
            f = open("./hg38.chrom.sizes")
            f_gc = open(gc_dir + "GC_percentage_" + str(resolution) + ".bed")

            outfile = "chr_chain_"+str(resolution)+".csv"
            for line in f.readlines():
                table = []

                chr_num, end = line.split()
                chr_num = chr_num[3:]
                print(f"Writing resoulution:{resolution}, chr:{chr_num} to csv file...")

                num = math.ceil(float(end) / float(resolution))
                end_res = num * resolution

                for i in range(num):
                    start_loc = 0 + i * resolution
                    end_loc = (i + 1) * resolution - 1
                    next_start_loc = start_loc + resolution
                    next_end_loc = end_loc + resolution
                    gc_line = f_gc.readline()

                    if i == num - 1:
                        end_loc = int(end)
                        next_start_loc = "NaN"
                        next_end_loc = "NaN"
                    _chr, _start, _end, gc_percentage = gc_line.split()
                    _chr = _chr[3:]
                    print(
                        f"chr: {chr_num}, _chr: {_chr}, start_loc: {start_loc}, _start: {_start}, end_loc: {end_loc}, _end: {_end}")
                    assert _chr == chr_num
                    assert _start == str(start_loc)

                    parent_start_loc = math.floor(float(start_loc) / float(prev_resolution)) * prev_resolution
                    table.append([chr_num, resolution, start_loc, end_loc, next_start_loc, next_end_loc, gc_percentage, prev_resolution, parent_start_loc])

                df = pd.DataFrame(table, columns=["chr", "resolution", "start_loc", "end_loc", "next_start_loc",
                                                  "next_end_loc", "GC_percentage", "prev_resolution",
                                                  "parent_start_loc"])
                if chr_num == "1":
                    df.to_csv(neo4j_home + outfile, index=False, mode='a+')
                else:
                    df.to_csv(neo4j_home + outfile, index=False, header=False, mode='a+')

            f_gc.close()
            prev_resolution = resolution
            f.close()

            print(f"Finished writing all chr_nums of resolution {resolution} to csv file {outfile}")


def create_chr_chain(outfile, uri, user, password, is50000):
    print("Starting to load csv file into database.")

    if is50000:
        print(f"Starting to load all nodes in {outfile}...")

        # load nodes
        graph = Graph(uri, user=user, password=password)
        graph.run("CREATE INDEX IF NOT EXISTS FOR (n:chr_chain) ON (n.chr, n.resolution, n.start_loc)")
        cypher = "LOAD CSV WITH HEADERS FROM 'file:///" + outfile +"' AS row\n"
        cypher += "MERGE (n:chr_chain {chr: toString(row.chr), resolution: toInteger(row.resolution), start_loc: toInteger(row.start_loc), end_loc: toInteger(row.end_loc) })\n"
        cypher += "SET n.GC_percentage = CASE row.GC_percentage WHEN 'NaN' THEN null ELSE toFloat(row.GC_percentage) END \n"
        cypher += "RETURN count(n)"
        graph.run(cypher=cypher)
        print(f"Finished loading all nodes in {outfile}")
        print(f"Start to load all edges in {outfile}...")

        # load edges
        cypher = "LOAD CSV WITH HEADERS FROM 'file:///" + outfile +"' AS row\n"
        cypher += "WITH row WHERE row.next_start_loc <> 'NaN'\n"
        cypher += "MATCH (n:chr_chain {chr: toString(row.chr), resolution: toInteger(row.resolution), start_loc: toInteger(row.start_loc)})\n"
        cypher += "MATCH (e:chr_chain {chr: toString(row.chr), resolution: toInteger(row.resolution), start_loc: toInteger(row.next_start_loc)})\n"
        cypher += "MERGE (n) - [:next_loc] -> (e)"
        graph.run(cypher=cypher)
        print(f"Finished loading all edges in {outfile}")
    else:
        graph = Graph(uri, user=user, password=password)

        for resolution in [10000, 5000, 1000, 200]:
        # for resolution in [10000, 5000, 1000]:
            outfile = "chr_chain_"+str(resolution)+".csv"

            # add all nodes
            print(f"Starting to load all nodes in {outfile}...")
            graph.run("CREATE INDEX IF NOT EXISTS FOR (n:chr_chain) ON (n.chr, n.resolution, n.start_loc)")
            cypher = "USING PERIODIC COMMIT 1000\n"
            cypher += "LOAD CSV WITH HEADERS FROM 'file:///" + outfile + "' AS row\n"
            cypher += "MERGE (n:chr_chain {chr: toString(row.chr), resolution: toInteger(row.resolution), start_loc: toInteger(row.start_loc), end_loc: toInteger(row.end_loc) })\n"
            cypher += "SET n.GC_percentage = CASE row.GC_percentage WHEN 'NaN' THEN null ELSE toFloat(row.GC_percentage) END \n"
            cypher += "RETURN count(n)"
            graph.run(cypher=cypher)
            print(f"Finished loading all nodes in {outfile}")

            # add all next_loc edges
            print(f"Start to load all next_loc edges in {outfile}...")
            cypher = "USING PERIODIC COMMIT 1000\n"
            cypher += "LOAD CSV WITH HEADERS FROM 'file:///" + outfile + "' AS row\n"
            cypher += "WITH row WHERE row.next_start_loc <> 'NaN'\n"
            cypher += "MATCH (n:chr_chain {chr: toString(row.chr), resolution: toInteger(row.resolution), start_loc: toInteger(row.start_loc)})\n"
            cypher += "MATCH (e:chr_chain {chr: toString(row.chr), resolution: toInteger(row.resolution), start_loc: toInteger(row.next_start_loc)})\n"
            cypher += "MERGE (n) - [:next_loc] -> (e)"
            graph.run(cypher=cypher)
            print(f"Finished loading all next_loc edges in {outfile}")

            # add all lower_resolution edges
            print(f"Start to load all lower_resolution edges in {outfile}...")
            cypher = "USING PERIODIC COMMIT 1000\n"
            cypher += "LOAD CSV WITH HEADERS FROM 'file:///" + outfile + "' AS row\n"
            cypher += "MATCH (n:chr_chain {chr: toString(row.chr), resolution: toInteger(row.resolution), start_loc: toInteger(row.start_loc)})\n"
            cypher += "MATCH (e:chr_chain {chr: toString(row.chr), resolution: toInteger(row.prev_resolution), start_loc: toInteger(row.parent_start_loc)})\n"
            cypher += "MERGE (n) - [:lower_resolution] -> (e)"
            graph.run(cypher=cypher)
            print(f"Finished loading all lower_resolution edges in {outfile}")


def write_and_load(uri, user, password, outfile, is50000, gc_dir, neo4j_home):
    # make_csv(gc_dir, is50000, outfile)
    create_chr_chain(outfile, uri, user, password, is50000)


def delete_all(uri, user, password):
    graph = Graph(uri, user=user, password=password)
    graph.delete_all()


if __name__ == "__main__":
    time_start = time.time()

    uri = "bolt://localhost:7687"
    user = 'neo4j'
    password = 'password'

    # gc_dir = "/nfs/turbo/umms-drjieliu/proj/genomeKG/data/hg38_assembly_NCBI/processed/"
    # gc_dir ="GC_content/"
    gc_dir = "/home/ltianjun/processed/"
    # neo4j_home = '/opt/neo4j/neo4j-community-4.2.4/import/'
    # neo4j_home = "~/Library/Application Support/Neo4j Desktop/Application/relate-data/dbmss/dbms-3e52da25-7ca5-4dc9-992b-068819174145/import/"
    neo4j_home = "./clean_csv/"

    write_and_load(uri, user, password, "chr_chain_50000.csv", True, gc_dir, neo4j_home)
    write_and_load(uri, user, password, "", False, gc_dir, neo4j_home)

    time_end = time.time()
    print('time cost: ', time_end - time_start, ' s')
