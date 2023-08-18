#!/usr/bin/env python
from ioutils import file_stream_reader, get_path_data
from parser import parse
from models import code_to_textual
import pprint

def get_parameters():
    params = {
        # "filename": "ClaroGT/ClaroGT_TelefoniaFija/TelefoniaFija/20230502123330_CLAROGTFIJA_GFTX230311_ver01.txt.err"
        'filename': "/home/vakord/Work/bill_parser/PROCESSED_20230502123330_CLAROGTFIJA_GFTX230311_ver01.txt"
    }
    return params


def get_bad_lines(bills):
    bad_bill_ranges = [range(bill.start_line, bill.end_line + 1) for bill in bills if any(bill.has_errors)]
    bad_lines = []
    
    for bad_range in bad_bill_ranges:
        bad_range = list(bad_range)
        bad_lines.extend(bad_range)
    
    return bad_lines


def purge_bad_lines(filename, bad_lines):
    path = get_path_data(filename)
    target = f"PROCESSED_{path.name}{path.suffix}"
    
    original = open(filename, "r")
    processed = open(target, "w")

    for index, line in enumerate(original):
        if index not in bad_lines:
            line = f"{line}"
            processed.write(line)
        
    original.close()
    processed.close()
    return


def print_errors(bills):
    total_errored = 0
    for i, bill in enumerate(bills):
        text_error, section_error = bill.has_errors
        
        if any([text_error, section_error]):
            total_errored += 1
            print(f"\nErrors for bill starting at line: {bill.start_line}")
            print(f"ending at line: {bill.end_line}")
        
        if section_error:
            print("\tSection errors found: ")
            textual_codes = code_to_textual(bill.missing_sections)
            print(f"\t\tBill has missing sections:  [{', '.join(textual_codes)}]")

        if text_error:
            print(f"\tTextual errors found: ")
            for error in bill.errors:
                print(f"\t\tError at line {error.line_index}, section: {error.line_section_index}: {error.section_name}\n\t\t{error.error}\n\t\toriginal predicate: \"{error.predicate}\"")

    print("\nTotal bills parsed: ", len(bills))
    print("Total bills with error: ", total_errored)


def print_bill_ranges(bills):
    for i, bill in enumerate(bills):
        print(f"{i + 1}.- Bill range = {bill.start_line, bill.end_line}")   


def main():
    params = get_parameters()
    filename = params["filename"]
    fp = file_stream_reader(filename)
    bills = parse(fp)

    print_errors(bills)
    # print_bill_ranges(bills)

    bad_lines = get_bad_lines(bills)
    purge_bad_lines(filename, bad_lines)


if __name__ == '__main__':
    main()

