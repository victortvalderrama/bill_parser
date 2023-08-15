#!/usr/bin/env python
from ioutils import file_stream_reader 
from parser import parse
from models import code_to_textual
import pprint

def get_parameters():
    params = {
        "filename": "ClaroGT/ClaroGT_TelefoniaFija/TelefoniaFija/20230502123330_CLAROGTFIJA_GFTX230311_ver01.txt.err"
    }
    return params

def main():
    params = get_parameters()  
    fp = file_stream_reader(params["filename"])
    bills = parse(fp)

    total_errored = 0

    for bill in bills:
        text_error, section_error = bill.bill_errors
        
        if any([text_error, section_error]):
            total_errored += 1
            print(f"\nErrors for bill starting at line: {bill.start_line_real}")
        
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

    return bills


if __name__ == '__main__':
    main()

