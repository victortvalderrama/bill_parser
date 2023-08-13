from collections import namedtuple

LineError = namedtuple(
    "Error",
    [
        "line_section_index",
        "predicate",
        "line_index",
        "error"
    ]
)

Line = namedtuple(
    "Line",
    [
        "section_index", 
        "section", 
        "sub_section", 
        "sub_sub_section", 
        "predicate", 
    ]
)


REQUIRED_SECTIONS = [
    "000101"
    "0002123"
    "0120301"
]

class Bill:

    def __init__(self):
        self.errors = []
        self.processed_lines = []


    def get_missing_sections(self):
        missing_sections = set(REQUIRED_SECTIONS) - set(self.processed_lines)
        return missing_sections