#!python3

def mean(values):
    """ Print mean of values. """
    mean = sum(values) / float(len(values))
    print("  Mean: {:.4f}".format(mean))


def median(values):
    """ Print median of values. """
    n = len(values)
    values.sort()

    if n % 2 == 0:
        median1 = values[n//2]
        median2 = values[n//2 - 1]
        median = (median1 + median2) / 2.0
    else:
        median = values[n//2]
    print("Median: {:.4f}".format(median))


def mode(values):
    """ Print mode of values. """
    from collections import Counter

    n = len(values)

    data = Counter(values)
    get_mode = dict(data)
    mode = ["{:.4f}".format(k) for k, v in get_mode.items() if v == max(list(data.values()))]

    if len(mode) == n:
        get_mode = "No mode found"
    else:
        get_mode = "  Mode: " + ', '.join(map(str, mode))

    print(get_mode)

def test():
    values = [0.34, 0.65, 0.73, 0.23, 0.18, 0.18, 0.89, 0.45, 0.45, 0.32, 0.56]
    mean(values)
    median(values)
    mode(values)

if __name__ == "__main__":
    test()