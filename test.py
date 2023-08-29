#!/usr/bin/env python
import re

def custom_tokenize(line):
    patterns = [
        r"\d{6}-\d{4}",  # Pattern like "007763-1233"
        r"\d{2}/\d{2}",  # Pattern like "02/07"
        r"\d+,?\d*\.\d{2}",  # Pattern like "1,612.00"
        r"\d{2}:\d{2}:\d{2}"  # Pattern like "16:58:07"
    ]

    combined_pattern = "|".join(patterns)
    matches = [(m.start(), m.end()) for m in re.finditer(combined_pattern, line)]
    
    new_tokens = []
    start = 0
    last_end = -1

    for s, e in matches:
        # Check for overlapping patterns based on start and end indices
        if s < last_end or (last_end > -1 and s < last_end and e > last_end):
            return f"Error: Overlapping patterns detected at index {s}"
        
        if start < s:
            token = line[start:s].strip()
            if token:
                new_tokens.append(token)
        
        new_tokens.append(line[s:e].strip())
        start = e
        last_end = e

    if start < len(line):
        token = line[start:].strip()
        if token:
            new_tokens.append(token)
    
    return new_tokens

line_with_overlap = "000172-181602/07 TV.CORP..HFC.AVANZADO 1,612.00 000172-1816 02/07 AMPLIFICADOR8A30VPCTV-4A 1,470.00"
result_with_overlap = custom_tokenize(line_with_overlap)
print("Custom Tokens:", result_with_overlap)