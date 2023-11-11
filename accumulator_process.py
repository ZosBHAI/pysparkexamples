def count_items(x):
    global cnt
    cnt += x


def process_data(rdd):
    rdd.foreach(count_items)
    return cnt.value