from helpers import remove_string_segments

line = "409/07/2023 13:36:46 1733                Telgua +5022361             6:00        0.00 | "
range_list = [(40,66),(123,149)]
tokens = remove_string_segments(line, range_list)
if len(tokens) % 5 != 0:
    print(tokens)
    
    # append_line_error(bill, parsed, line_index, "invalid number of rows, can't divide by 5")