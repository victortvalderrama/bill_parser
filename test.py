#!/usr/bin/env python
import re

def validate_tokens(tokens):
    # Patterns for each type of token
    patterns = [
        re.compile(r'^\d{2}/\d{2}$'),         # Date
        re.compile(r'^\d{2}:\d{2}:\d{2}$'),   # Time
        re.compile(r'^\d+$'),                 # Phone number
        re.compile(r'^[A-Za-z\s]+$'),         # Operator name
        re.compile(r'^\d+$'),                 # Call count
        re.compile(r'^\d+\.\d{2}$'),          # Call cost
    ] * (len(tokens) // 6) # Repeat the pattern for each group

    for token, pattern in zip(tokens, patterns):
        if not pattern.match(token):
            print(f"Token '{token}' does not match expected pattern.")
            return False

    return True


tokens_to_check = ["13/02", "16:54:11", "38165357CLARO","3", "0.00","17/02", "17:07:59", "40510943", "OTRO OPERADOR", "2", "0.00"]
is_valid = validate_tokens(tokens_to_check)
print(is_valid) # Output will be False # Output will be False and print the error message

