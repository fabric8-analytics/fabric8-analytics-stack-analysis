def generate_value_list_from_dict(dictionary):
    """
    Creates a list of all the values of a dict after de-dup

    :param dictionary: dict object
    :return: list of all values de-duped
    """
    value_set = set()
    for key in dictionary:
        value_set |= set(dictionary[key])
    value_list = list(value_set)
    return value_list


def generate_key_to_value_edges(dictionary):
    """
    Represents every (key,[value1,value2,....]) pair of the dict as a list of edges [{"from":key,"to":value1},{"from":key,"to":value2},.....]

    :param dictionary: dict object
    :return: list of edges where edges are represented as tuples
    """

    list_edge_dict = list()

    for key in dictionary:

        for value in dictionary[key]:
            edge_dict = dict()
            edge_dict["from"] = key
            edge_dict["to"] = value
            list_edge_dict.append(edge_dict)
    return list_edge_dict
