import ast

import numpy as np


def generate_list_of_tuples_from_string(string):
    return list(ast.literal_eval(string))


def generate_evidence_map_from_transaction_list(list_transaction):
    return {transaction: 1 for transaction in list_transaction}


def generate_kronos_dependency_list_for_pomegranate(string):
    return tuple(generate_list_of_tuples_from_string(string))


def generate_matrix_from_pandas_df(df, node_list):
    x = df.as_matrix(columns=node_list)
    y = np.vstack((x, np.ones((1, len(node_list)))))
    result = np.vstack((y, np.zeros((1, len(node_list)))))
    return result


def trunc_string_at(s, d, n1, n2):
    """
    Returns s truncated at the n'th occurrence of the delimiter, d
    """
    if n2 > 0:
        result = d.join(s.split(d, n2)[n1:n2])
    else:
        result = d.join(s.split(d, n2)[n1:])
        if not result.endswith("/"):
            result += "/"
    return result
