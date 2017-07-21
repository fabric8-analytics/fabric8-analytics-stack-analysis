def trunc_string_at(s, d, n1, n2):
    """Returns s truncated at the n'th occurrence of the delimiter, d"""
    if n2 > 0:
        result = d.join(s.split(d, n2)[n1:n2])
    else:
        result = d.join(s.split(d, n2)[n1:])
        if not result.endswith("/"):
            result += "/"
    return result
