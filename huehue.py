def extract_sections(input_string, ranges):
    sections = []
    for start, end in ranges:
        section = input_string[start:end]
        sections.append(section)
    return sections

# Example usage:
input_string = "                                      1231aaaaaaaaaaaaaaaaaaaaaaaahh                                                              aaaaaaahhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh                                                ."
ranges = [(40, 65), (127, 153)]

sections = extract_sections(input_string, ranges)
print(sections)