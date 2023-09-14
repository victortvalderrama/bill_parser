from models import LineError

from functools import partial
from collections import namedtuple
import re

def validate_tokens(tokens, expected_patterns):
    errors = []

    if len(tokens) < len(expected_patterns):
        errors.append(f"Tokenized predicate of unexpected len, expecting: {len(expected_patterns)}, found: {len(tokens)}")
        return errors

    for i, ep in enumerate(expected_patterns):
        token = tokens[i]
        pattern = ep[0]
        error_str = ep[1]

        if type(pattern) == re.Pattern:        
            if not pattern.match(token):
                errors.append(error_str)
        
        elif type(pattern) == str:
            if not pattern == token:
                errors.append(error_str)

        else:
            raise ValueError(f"Got unexpected pattern '{pattern}' of type '{type(pattern)}'")
    return errors

def undefined_line(line_index, parsed):
    # print(f"Undefined behaviour at l: {line_index} for line id: {parsed.section_index}")
    # print(f"Predicate is: {parsed.predicate}")
    pass

def generic_predicate(bill, line_index, parsed, field_name):
    value = parsed.predicate.strip() + f"#{line_index}"
    setattr(bill, field_name, value)
    
def append_line_error(bill, parsed, line_index, error_msg):
    section_name = getattr(bill.SECTION_NAME_MAP, parsed.section_index, "UNKNOWN SECTION")
    error = LineError(
        line_section_index=parsed.section_index,
        section_name=section_name,
        predicate=parsed.predicate,
        line_index=line_index,
        error=error_msg
    )
    bill.errors.append(error) 
   
def split_predicate(line_index, parsed, bill, split_len):
    tokens = parsed.predicate.split()

    if len(tokens) > split_len:
        error_msg = f"Could not split line in {split_len}"
        append_line_error(bill, parsed, line_index, error_msg)
        return None

    return tokens

def is_four_digits(token):
    pattern = r"^\d{4}$"
    return bool(re.match(pattern, token))

def remove_string_segments(string, range_list):
    res = ""
    for idx, chr in enumerate(string):
        for strt_idx, end_idx in range_list:
            if strt_idx <= idx +1 <= end_idx:
                break
        else:
            res += chr
    return res.split()