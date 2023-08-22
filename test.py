#!/usr/bin/env python

import re

def combine_tokens(string):
    tokens = string.split()

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

# Usage
string = "02/07 15:04:48 19542580900 USO RED CLARO 1 0.46 02/07 15:04:48 19542580900 ESTADOS UNIDO 1 3.58"
result = combine_tokens(string)
print(result)