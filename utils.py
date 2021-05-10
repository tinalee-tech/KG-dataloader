import re
import string
from copy import deepcopy
import math


def find_start_loc(pos, resolution):
    temp = math.floor(float(int(pos) - 1) / float(resolution)) * float(resolution) + 1
    return int(temp)


def convert(data_type):
    """
    Given a string indicating the data type, returns the type function

    Args:
        data_type: a string indicating the data type (e.g. 'str', 'int')

    Return:
        the type function
    """
    if data_type == 'str':
        return str
    elif data_type == 'int':
        return int
    elif data_type == 'float':
        return float


def convert_attribute(col):
    """
    Convert column names into viable neo4j attribute names
    i.e. Tokens inside of braces will be deleted, all special characters will be deleted,
    spaces will be replaced by '_'

    Args:
        col: the pre-processed column name

    Return:
        the final attribute name that will be used in neo4j database
    """

    # delete whatever is in ()
    col = re.sub("\(.*\)", "", col)
    col = col.strip()
    # replace all punctuation with space
    for char in string.punctuation:
        col = col.replace(char, " ")
    # merge continuous spaces into one, then replace all spaces with "_"
    return ' '.join(col.split()).replace(" ", "_").lower()


def match_re_nodes(pattern, s, nodes_and_edges):
    """
    Match a given pattern inside a string and use the match result to update the attributes of nodes and edges

    Args:
        pattern: function "parse_re"'s output re pattern
        s: (str) the given string
        nodes_and_edges: (dict) the dict recording all node and edge information

    Return:
        nodes_and_edges: (dict) the updated nodes_and_edges
    """
    result = re.match(pattern[0], s)
    group_num = len(result.groups())
    res = ''
    for i in range(len(pattern[1])):
        _info = pattern[1][i]  # ({node/edge name}, {attribute name}, {data type})
        if _info[0] not in nodes_and_edges:
            continue
        if _info[1] not in nodes_and_edges[_info[0]]:
            continue
        _type = nodes_and_edges[_info[0]][_info[1]][1]
        if group_num <= i:
            # a single column corresponds to two label's attribute
            nodes_and_edges[_info[0]][_info[1]] = (res, _type)
        else:
            res = convert(_type)(result.group(i + 1))
            nodes_and_edges[_info[0]][_info[1]] = (res, _type)
    return nodes_and_edges


def parse_re_nodes(list_s):
    """
    Using a given pattern from .format file to generate the re expression

    Args:
        list_s: a list of strings e.g., [{A.chr}_{A.pos}_{A.ref}_{A.alt}_b38]

    Return:
        pattern: an re pattern + the attributes

    Example:
        > parse_re_nodes('{A.chr}_{A.pos}_{A.ref}_{A.alt}_b38')
        > (re.compile('^(.*?)_(.*?)_(.*?)_(.*?)_b38$'), [('A', 'chr'), ('A', 'pos'), ('A', 'ref'), ('A', 'alt')])

        > parse_re_nodes('{B.id}.{B.id_version}')
        > (re.compile('^(.*?)\.(.*?)$'), [('B', 'id'), ('B', 'id_version')])
    """
    final_pat = ''
    attributes_info = []
    temp = ''
    inside = False
    for i, s in enumerate(list_s):
        pat = ''  # the start and end of a string
        for elm in s:
            if elm == '{':
                inside = True
            elif elm == '}':
                inside = False
                _info = tuple(temp.split('.'))
                attributes_info.append(_info)
                temp = ''
                pat += '(.*?)'  # non-greedy
            elif inside:
                temp += elm
            elif elm in '.?*&^%$#()':
                pat += '\\' + elm
            else:
                pat += elm
        pat = '^' + pat + '$'
        if i == 0:
            final_pat = pat
    return re.compile(final_pat), attributes_info


def match_re_global(pattern, attributes_info, s):
    """
    Find a given pattern from the file name, return global vars

    Args:
        pattern: function "parse_re_global"'s output re pattern
        attributes_info: (dict) the information of the corresponding attributes ({node/edge name}, {attribute name})
            e.g., [('B', 'id'), ('B', 'id_version')]
        s: (str) the giving string

    Return:
        global_variables (list)
    """
    global_variables = []
    if pattern[0] == None:
        return global_variables
    result = re.match(pattern[0], s)
    for i, name in enumerate(pattern[1]):
        node, attr, _type = attributes_info[name]  # ({node/edge name}, {attribute name}, {data type})
        res = convert(_type)(result.group(i + 1))
        global_variables.append((node, attr, res, _type))
    return global_variables


def parse_re_global(s):
    """
    Similar with the previous function but applies to global info (e.g., file names)

    E.g.,
        >  parse_re_global('{tissue}.v{version}.signif_variant_gene_pairs.txt.gz')
        > (re.compile('^(.*?)\.v(.*?)\.signif_variant_gene_pairs\.txt\.gz$'))
    """
    pat = ''  # the start and end of a string
    attributes_info = []
    temp = ''
    inside = False
    for elm in s:
        if elm == '{':
            inside = True
        elif elm == '}':
            inside = False
            attributes_info.append(temp)
            temp = ''
            pat += '(.*?)'  # non-greedy
        elif inside:
            temp += elm
        elif elm in '.?*&^%$#()':
            pat += '\\' + elm
        else:
            pat += elm
    pat = '^' + pat + '$'
    return re.compile(pat), attributes_info


def parse_nodes_edges(s):
    """
    From the information, generate the dict for nodes and edges

    E.g.,
        >  parse_nodes_edges('node:A{label = variant}node:B{label = gene}edge:C{label = correlated with,from = A,to = B}')
    """
    current = None
    temp = ''
    nodes_and_edges = {}
    for i in range(len(s)):
        if s[i] == '{':
            [tp, name] = [elm.strip() for elm in temp.split(':')]
            nodes_and_edges[name] = {'node_or_edge': tp}
            current = name
            temp = ''
        elif s[i] == '}':
            attrs = [elm.strip() for elm in temp.split(',')]
            for attr in attrs:
                [k, v] = [elm.strip() for elm in attr.split('=')]
                nodes_and_edges[current][k] = v
            temp = ''
        else:
            temp += s[i]
    for n in nodes_and_edges:
        assert 'label' in nodes_and_edges[n]
        if nodes_and_edges[n]['node_or_edge'] == 'edge':
            assert 'from' in nodes_and_edges[n]
            assert 'to' in nodes_and_edges[n]
    return nodes_and_edges


if __name__ == '__main__':
    x, y = parse_re_nodes('{A.chr}_{A.pos}_{A.ref}_{A.alt}_b38')
    print(x)
    print(y)

    x, y = parse_re_global('{tissue}.v{version}.signif_variant_gene_pairs.txt.gz')
    print(x)
    print(y)

    result = re.match(x, 'Spleen.v8.signif_variant_gene_pairs.txt.gz')
    print(result.group(1))

    x = parse_nodes_edges('node:A{label = variant}node:B{label = gene}edge:C{label = correlated with,from = A,to = B}')
    print(x)



