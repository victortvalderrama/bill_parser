from functools import partial
from collections import namedtuple
from models import LineError, SECTION_NAME_MAP
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
   
def split_predicate(line_index, parsed, bill, split_len):
    tokens = parsed.predicate.split()

    if len(tokens) > split_len:
        error_msg = f"Could not split line in {split_len}"
        append_line_error(bill, parsed, line_index, error_msg)
        return None

    return tokens

def combine_tokens(predicate):
    tokens = predicate.split()

    patterns = [
        r"\d{2}/\d{2}",
        r"\d{2}:\d{2}:\d{2}",
        r"\d+",
        r"\d+\.\d+"
    ]
    combined_tokens = []
    current_combined = ""
    for token in tokens:
        if any(re.match(pattern, token) for pattern in patterns):
            if current_combined:
                combined_tokens.append(current_combined)
                current_combined = ""
            combined_tokens.append(token)
        else:
            current_combined += " " + token

    if current_combined:
        combined_tokens.append(current_combined.strip())

    return combined_tokens

def is_four_digits(token):
    pattern = r"^\d{4}$"
    return bool(re.match(pattern, token))

def custom_tokenize(line):
    # Define the regex patterns to match, in priority order
    patterns = [
        r"\d{6}-\d{4}",  # Pattern like "007763-1233"
        r"\d{2}/\d{2}",  # Pattern like "02/07"
        r"\d+,?\d*\.\d{2}",  # Pattern like "1,612.00"
        r"\d{2}:\d{2}:\d{2}"  # Pattern like "16:58:07"
    ]

    # Combine all patterns into a single regex pattern
    combined_pattern = "|".join(patterns)

    # Use regex finditer to match all patterns and get their start and end indices
    matches = [(m.start(), m.end()) for m in re.finditer(combined_pattern, line)]

    # Initialize the list to store new tokens
    new_tokens = []

    # Initialize start index for unmatched tokens
    start = 0

    for s, e in matches:
        # If there's an unmatched token before this match, add it to new_tokens
        if start < s:
            token = line[start:s].strip()
            if token:  # Only append non-empty tokens
                new_tokens.append(token)
        
        # Add the matched token to new_tokens
        new_tokens.append(line[s:e].strip())

        # Update start index
        start = e

    # Add any remaining unmatched token to new_tokens
    if start < len(line):
        token = line[start:].strip()
        if token:  # Only append non-empty tokens
            new_tokens.append(token)

    return new_tokens