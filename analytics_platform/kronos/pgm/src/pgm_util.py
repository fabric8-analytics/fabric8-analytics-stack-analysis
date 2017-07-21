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
