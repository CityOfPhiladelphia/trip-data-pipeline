from itertools import zip_longest


def grouper(n, iterable, fillvalue=None):
    """
    Yield groups of size n from the iterable (e.g.,
    grouper(3, 'ABCDEFG', 'x') --> ABC DEF Gxx").
    Pulled from itertools recipes.
    """
    args = [iter(iterable)] * n
    return zip_longest(fillvalue=fillvalue, *args)


