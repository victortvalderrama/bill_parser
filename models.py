from collections import namedtuple

LineError = namedtuple(
    "Error",
    [
        "line_section_index",
        "section_name",
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

SECTION_NAME_MAP = {
    "0100000": "START DELIMTER",
    "5000100": "END DELIMITER"
}

REQUIRED_SECTIONS_AT_LEAST_ONCE = [
    "0100000",
    "5000100",
]

REQUIRED_SECTIONS_AT_LEAST_TWICE = [
    "",
]


def code_to_textual(codes):
    return [f"{section_code}: {SECTION_NAME_MAP[section_code]}" for section_code in codes]



class Bill:

    def __init__(self, starting_line_human, start_line_real):
        self.errors = []
        self.processed_lines = []
        self.starting_line_human = starting_line_human
        self.start_line_real = start_line_real
        self.ending_line = -1

    @property
    def missing_sections(self):
        missing_sections = set(REQUIRED_SECTIONS_AT_LEAST_ONCE) - set(self.processed_lines)
        return missing_sections
    
    @property
    def bill_errors(self):
        text_errors = True if len(self.errors) > 0 else False
        section_errors = True if len(self.missing_sections) > 0 else False
        return text_errors, section_errors
    