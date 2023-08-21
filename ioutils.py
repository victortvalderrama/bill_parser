from constants import BAD_SUFFIXES
from pathlib import Path
from models import PathData

def file_stream_reader(filename):
    with open(filename, "r", encoding="ISO-8859-1") as fp:
        line = fp.readline()
        while line:
            yield line.rstrip()
            line = fp.readline()


def get_path_data(filename):    
    p = Path(filename)
    if len(p.suffixes) > 1:
        suffix = set(p.suffixes) - set(BAD_SUFFIXES)
        if len(suffix) == 1:
            suffix = list(suffix)[0]
        else:
            suffix = p.suffix
    else:
        suffix = p.suffix
    
    name = p.name.replace("".join(p.suffixes), "")
    return PathData(name=name, suffix=suffix)
