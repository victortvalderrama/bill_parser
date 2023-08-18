from functools import partial
from collections import namedtuple
from models import LineError, PathData, SECTION_NAME_MAP
import re

def validate_tokens(tokens, expected_patterns):
    errors = []

    if tokens is None or len(tokens) < len(expected_patterns):
        for i in range(len(tokens) if tokens else 0, len(expected_patterns)):
            _, error_message = expected_patterns[i]
            errors.append(error_message)
        return errors

    for i, (pattern, error_message) in enumerate(expected_patterns):
        token = tokens[i]
        if isinstance(pattern, str) and not re.fullmatch(pattern, token):
            errors.append(error_message)
        elif isinstance(pattern, list) and token not in pattern:
            errors.append(error_message)

    return errors

def undefined_line(line_index, parsed):
    # print(f"Undefined behaviour at l: {line_index} for line id: {parsed.section_index}")
    # print(f"Predicate is: {parsed.predicate}")
    pass

def generic_predicate(bill, line_index, parsed, field_name):
    value = parsed.predicate.strip() + f"#{line_index}"
    setattr(bill, field_name, value)
    
def append_line_error(bill, parsed, line_index, error_msg):
    section_name = getattr(SECTION_NAME_MAP, parsed.section_index, "UNKNOWN SECTION")
    error = LineError(
        line_section_index=parsed.section_index,
        section_name=section_name,
        predicate=parsed.predicate,
        line_index=line_index,
        error=error_msg
    )
    bill.errors.append(error) 
   
def split_predicate(line_index, parsed, bill, split_len): #, looking_list=None):
    tokens = parsed.predicate.split()
    
    # if looking_list:
    #     tokens = rebuild_searching_list(looking_list, tokens)
        
    #     if not validate_tokens(tokens):
    #         error_msg = "One or more tokens do not match the expected patterns."
    #         append_line_error(bill, parsed, line_index, error_msg)
    #         return None

    if len(tokens) > split_len:
        error_msg = f"Could not split line in {split_len}"
        append_line_error(bill, parsed, line_index, error_msg)
        return None

    return tokens

def rebuild_searching_list(looking_list, tokens):
    rebuilt_tokens = []
    i = 0
    while i < len(tokens):
        found = False
        for look in looking_list:
            look_parts = look.split()
            if tokens[i:i + len(look_parts)] == look_parts:
                rebuilt_tokens.append(look)
                i += len(look_parts)
                found = True
                break
        if not found:
            rebuilt_tokens.append(tokens[i])
            i += 1
    return rebuilt_tokens