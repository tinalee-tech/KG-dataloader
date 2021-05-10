from utils import parse_re_global, parse_re_nodes, parse_nodes_edges, convert


def parse_format(format_file):
    """
    Parse information from the .format file of tabular/matrix data

    Args:
        format_file: .format file path

    Return:
        n_headers: number of header lines
        file_name_pattern: re pattern for file names (might contain some global variables)
                e.g., (re.compile('^(.*?)\\.v(.*?)\\.signif_variant_gene_pairs\\.txt\\.gz$'),
                    ['tissue', 'version'])
        nodes_and_edges: all nodes and edges information (connection / label / attributes)
                e.g., {'A':{
                        'node_or_edge': 'node', 'label': 'variant',  # these two always exist!
                        'chr': (None, 'str'), 'pos': (None, 'int')  # (default data, data type)
                    },
                    'C':{
                        'node_or_edge': 'edge', 'label': 'correlated with', 'from': 'A', 'to': 'B',  # these five always exist for edges!
                        'caller_version': ('0.5', 'str'), 'distance': (None, 'int'),
                    }
                }
        global_vals: global vars that's loaded from file names
                e.g., {'tissue':
                    ('C', 'tissue', 'str')  # (node/edge name, attribute, data type)
                }
        columns: the formats for each column
                e.g. [(re.compile('^(.*?)_(.*?)_(.*?)_(.*?)_b38$'), [('A', 'chr'), ('A', 'pos'), ('A', 'ref'), ('A', 'alt')]),
                        (re.compile('^(.*?)$'), [('B', 'name')])]
        node_primary_key: (dict) a dictionary whose key is node name, value is a list of primary key attribute of that node
                e.g. {'A':['id'], 'B':['name']}
        B_col_prop: boolean variable checking whether or not the data file provided is a matrix file.
                If it is matrix file, return as True. Else, return as False.
    """
    # record all required information
    info_included = {elm: False for elm in [
        'format_type', 'file_name', 'headers', 'delimiter', 'graph_pattern', 'line_format',
        'global_variables', 'node_attributes', 'node_primary_keys', 'chr_chain'
    ]} #add in extra format info

    n_headers = 0
    file_name_pattern = (None, None)
    global_vals = {}
    columns = []  # column formats
    node_primary_key = {}
    format_type = 0
    B_col_prop = False
    chr_chain_info = {}
    delimiter = ''

    # temporary variables
    attr_types = []
    node_lines = ''
    fixed_global_vars = []

    current_info = None
    for line in open(format_file):
        line = line.split('#')[0].strip()   #could start with no #
        line = bytes(line, 'utf-8').decode('unicode_escape')
        if len(line) == 0:
            continue

        elif line.startswith('>'):
            current_info = line[1:]
            if current_info not in info_included:
                print(f'Warning: {current_info} not in the required information list!')

        elif current_info not in info_included:
            continue

        elif current_info == 'format_type':
            if info_included[current_info]:
                raise ValueError(f'{current_info} more than 1 line!')
            try:
                format_type = int(line)
            except ValueError:
                raise ValueError(f'{current_info} has to be integer value! (0: tabular, 1: matrix)')
            info_included[current_info] = True  # already loaded

        elif current_info == 'headers':
            if info_included[current_info]:
                raise ValueError(f'{current_info} more than 1 line!')
            n_headers = int(line)
            info_included[current_info] = True  # already loaded

        elif current_info == 'file_name':
            if info_included[current_info]:
                raise ValueError(f'{current_info} more than 1 line!')
            if '{' in line and '}' in line:
                file_name_pattern = parse_re_global(line)
            info_included[current_info] = True

        elif current_info == 'delimiter':
            if info_included[current_info]:
                raise ValueError(f'{current_info} more than 1 line!')
            info_included[current_info] = True
            delimiter = line

        elif current_info == 'graph_pattern':
            info_included[current_info] = True
            node_lines += line

        elif current_info == 'line_format':
            info_included[current_info] = True
            # TODO: one column correspond to two label's attributes
            _temp = line.split()
            line_pattern = []
            for i, _string in enumerate(_temp):
                if i == 0:
                    _colname = _string
                else:
                    line_pattern.append(_string)
            if 'format_type' not in info_included.keys():
                raise ValueError('format_type has to be defined before line_format!')
            if format_type == 1 and _colname == 'others':
                B_col_prop = True
            if '{' in line and '}' in line_pattern[0]:
                columns.append(parse_re_nodes(line_pattern))
            else:
                columns.append(None)  # this line is not important

        elif current_info == 'global_variables':
            info_included[current_info] = True
            try:
                [_attribute, _others] = [elm.strip() for elm in line.split('=')]
            except Exception:
                print(f'Warning: In chr_chain field, \"{line}\" not in the correct format. This line will be discarded.')
                continue
            [node, attr] = _attribute.split('.')
            [_var_name, _type] = _others.split()
            if '{' in _var_name and '}' in _var_name:  # dynamic
                _var_name = _var_name[1:-1]
                _type = _type[1:-1]
                if _var_name not in file_name_pattern[1]:
                    raise ValueError(f'Global var {_var_name} cannot be loaded from any places!')
                global_vals[_var_name] = (node, attr, _type)
            else:  # fixed global vars
                _type = _type[1:-1]
                if _var_name == 'x':
                    fixed_global_vars.append((node, attr, _var_name, _type))
                else:
                    fixed_global_vars.append((node, attr, convert(_type)(_var_name), _type))

        elif current_info == 'node_attributes':
            info_included[current_info] = True
            [_attribute, _type] = line.split()
            [node, attr] = _attribute.split('.')
            _type = _type[1:-1]
            attr_types.append((node, attr, _type))

        elif current_info == 'node_primary_keys':
            info_included[current_info] = True
            [_attribute, _type] = line.split()
            [node, attr] = _attribute.split('.')
            _type = _type[1:-1]
            if node not in node_primary_key.keys():
                node_primary_key[node] = [attr]
            else:
                node_primary_key[node].append(attr)

        elif current_info == 'chr_chain':
            info_included[current_info] = True
            try:
                [_chr_attribute, _pos_attribute, resolution] = line.split()
            except Exception:
                print(f'Warning: In chr_chain field, \"{line}\" not in the correct format. This line will be discarded.')
                continue
            [node, chr] = _chr_attribute.split('.')
            [_temp, pos] = _pos_attribute.split('.')
            if _temp != node:
                raise ValueError('In chr_chain field, the same line\'s chr and pos attributes\' label has to match.')
            chr_chain_info[node] = {}
            chr_chain_info[node]["chr"] = chr
            chr_chain_info[node]["pos"] = pos
            chr_chain_info[node]["resolution"] = resolution

        else:
            pass

    if format_type == 1 and B_col_prop == False:
        raise ValueError('The last line of line_format section must be of format: others {B.attribute_name}')

    nodes_and_edges = parse_nodes_edges(node_lines)
    for node in nodes_and_edges.keys():
        if nodes_and_edges[node]['node_or_edge'] == 'node' and node not in node_primary_key.keys():
            raise ValueError(f'Lack primary key for node {node}')
        if nodes_and_edges[node]['node_or_edge'] == 'edge' and node in node_primary_key.keys():
            raise ValueError(f'Should specify no primary key for edge {node}')
    for (node, attr, _default, _type) in fixed_global_vars:
        nodes_and_edges[node][attr] = (_default, _type)

    for (node, attr, _type) in attr_types:
        nodes_and_edges[node][attr] = (None, _type)

    for (node, attr, _type) in global_vals.values():
        nodes_and_edges[node][attr] = (None, _type)

    for elm in info_included:
        if not info_included[elm]:
            raise ValueError(f'Missing: {elm}')

    return n_headers, file_name_pattern, nodes_and_edges, global_vals, columns, node_primary_key, B_col_prop, \
           chr_chain_info, delimiter


if __name__ == '__main__':
    n_headers, file_name_pattern, nodes_and_edges, global_vals, columns = parse_format('GTEx.format')

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
    print('Columns:')
    for i, column in enumerate(columns):
        print('Column', i, ':', column)




