"""Various PGM-related utility functions."""

import ast
import numpy as np
import six


def generate_list_of_tuples_from_string(string):
    """Generate list of tuples from given input string that is evaluated via AST module."""
    return list(ast.literal_eval(string))


def generate_evidence_map_from_transaction_list(list_transaction):
    """Generate a map with keys taken from input list and values set to 1."""
    return {transaction: 1 for transaction in list_transaction}


def generate_kronos_dependency_list_for_pomegranate(string):
    """Generate Kronos dependency list for Pomegranate."""
    return tuple(generate_list_of_tuples_from_string(string))


def generate_matrix_from_pandas_df(df, node_list):
    """Generate matrix from Pandas data frame."""
    start = df.shape[0]
    # Append a row containing all zeroes, else the inference may fail
    df.loc[start] = np.zeros(len(node_list))
    # Append a list of all ones, else the inference may fail
    df.loc[start + 1] = np.ones(len(node_list))
    if six.PY2:
        # Backward compatability with python2 pomegranate.
        node_list = [str(node_name) for node_name in node_list]
    return df.as_matrix(columns=node_list)
