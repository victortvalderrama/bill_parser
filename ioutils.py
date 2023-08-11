def file_stream_reader(filename):
    with open(filename, "r") as fp:
        line = fp.readline()
        while line:
            yield line.rstrip()
            line = fp.readline()