
from functools import partial
from collections import namedtuple
from models import LineError, SECTION_NAME_MAP
import re

def undefined_line(line_index, parsed):
    # print(f"Undefined behaviour at l: {line_index} for line id: {parsed.section_index}")
    # print(f"Predicate is: {parsed.predicate}")
    pass

def file_stream_reader(filename):
    with open(filename, "r") as fp:
        line = fp.readline()
        while line:
            yield line.rstrip()
            line = fp.readline()
            
def generic_predicate(bill, line_index, parsed, field_name):
    value = parsed.predicate.strip() + f"#{line_index}"
    setattr(bill, field_name, value)
    
def validate_tokens(tokens):
    patterns = [
        re.compile(r'^\d{2}/\d{2}$'),         # Date
        re.compile(r'^\d{2}:\d{2}:\d{2}$'),   # Time
        re.compile(r'^\d+$'),                 # Phone number
        # re.compile(r'^(COBRO REV\.FY|SERV\.CLIENTE)$'), 
        re.compile(r'^[A-Za-z\s]+$'),         # Operator name
        re.compile(r'^\d+$'),                 # Call count
        re.compile(r'^\d+\.\d{2}$'),          # Call cost
    ] * (len(tokens) // 6)

    for token, pattern in zip(tokens, patterns):
        if not pattern.match(token):
            print(token)
            return False

    return True
    
def split_predicate(line_index, parsed, bill, split_len, looking_list=None):
    tokens = parsed.predicate.split()
    if looking_list:
        tokens = rebuild_searching_list(looking_list, tokens)
        
        if not validate_tokens(tokens):
            error_msg = "One or more tokens do not match the expected patterns."
            section_name = getattr(SECTION_NAME_MAP, parsed.section_index, "UNKNOWN SECTION")
            error = LineError(
                line_section_index=parsed.section_index,
                section_name=section_name,
                predicate=parsed.predicate,
                line_index=line_index,
                error=error_msg
            )
            bill.errors.append(error)
            return None

    if len(tokens) > split_len:
        error_msg = f"Could not split line in {split_len}"
        section_name = getattr(SECTION_NAME_MAP, parsed.section_index, "UNKNOWN SECTION")
        error = LineError(
            line_section_index=parsed.section_index,
            section_name=section_name,
            predicate=parsed.predicate,
            line_index=line_index,
            error=error_msg
        )
        bill.errors.append(error)
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


# def split_predicate(line_index, parsed, bill, split_len):
#     tokens = parsed.predicate.split()
#     if len(tokens) > split_len:
#         error_msg = f"Could not split line in {split_len}"

#         section_name = getattr(SECTION_NAME_MAP, parsed.section_index, "UNKNOWN SECTION")

#         error = LineError(
#             line_section_index=parsed.section_index,
#             section_name=section_name,
#             predicate=parsed.predicate,
#             line_index=line_index,
#             error=error_msg
#         )

#         bill.errors.append(error)
#         return None
#     return tokens