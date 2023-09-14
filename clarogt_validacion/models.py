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

PathData = namedtuple(
    "PathData",
    [
        "name",
        "suffix",
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

FIXED_SECTION_NAME_MAP = {
    "0100000": "START DELIMITER",
    "0800100": " MIDDLE JOKER",
    "5000100": "END DELIMITER",
}

FIXED_REQUIRED_SECTIONS_AT_LEAST_ONCE = [
    "0100000",
    "0100100",
    "0100200",
    "0100300",
    "0100400",
    "0100500",
    "0100600",
    "0100700",
    "0100800",
    "0100900",
    "0101000",
    "0101200",

    "0200000",
    "0200100",
    "0200200",
    "0200300",
    "0200400",
    "0200500",
    "0200600",

    # "0300100",
    # "0300110",
    # "0300150",

    "5000100",
]

MOBILE_SECTION_NAME_MAP = {
    "100000": "START DELIMITER",
    # "0800100": " MIDDLE JOKER",
    "999999": "END DELIMITER",
}

MOBILE_REQUIRED_SECTIONS_AT_LEAST_ONCE = [
    
]


def code_to_textual(codes):
    return codes
    # return [f"{section_code}: {SECTION_NAME_MAP[section_code]}" for section_code in codes]



class Bill:

    def __init__(self, start_line):
        self.errors = []
        self.processed_lines = []
        self.start_line = start_line
        self.end_line = -1
        
        self.first_0800100_encountered = False

    @property
    def missing_sections(self):
        missing_sections = set(self.REQUIRED_SECTIONS_AT_LEAST_ONCE) - set(self.processed_lines)
        return missing_sections

    
    @property
    def has_errors(self):
        text_errors = True if len(self.errors) > 0 else False
        section_errors = True if len(self.missing_sections) > 0 else False
        return text_errors, section_errors


class FixedBill(Bill):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.REQUIRED_SECTIONS_AT_LEAST_ONCE = FIXED_REQUIRED_SECTIONS_AT_LEAST_ONCE
        self.SECTION_NAME_MAP = FIXED_SECTION_NAME_MAP

class MobileBill(Bill):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.REQUIRED_SECTIONS_AT_LEAST_ONCE = MOBILE_REQUIRED_SECTIONS_AT_LEAST_ONCE
        self.SECTION_NAME_MAP = MOBILE_SECTION_NAME_MAP
