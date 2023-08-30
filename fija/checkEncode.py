#!/usr/bin/env python
import chardet

def check_file_encoding(filename):
    with open(filename, 'rb') as f:
        rawdata = f.read()
        result = chardet.detect(rawdata)
        return result['encoding']

def change_file_encoding(input_filename, output_filename, target_encoding):
    with open(input_filename, 'rb') as input_file:
        content = input_file.read()
    
    with open(output_filename, 'wb') as output_file:
        output_file.write(content.decode(encoding).encode(target_encoding))

# Example usage
input_filename = 'PROCESSED_GOOD_20230822233841_CLAROGTFIJA_GFTX230858.txt'
target_encoding = 'ISO-8859-1'  # The desired encoding
output_filename = 'output_encoded.txt'

encoding = check_file_encoding(input_filename)
print(f"The file '{input_filename}' is encoded in '{encoding}'")

# Change the encoding and save to a new file
change_file_encoding(input_filename, output_filename, target_encoding)
print(f"The file '{output_filename}' has been encoded in '{target_encoding}'")