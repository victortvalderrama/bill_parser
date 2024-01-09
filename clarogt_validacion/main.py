#!/usr/bin/env python
import argparse
import os
from ioutils import file_stream_reader, get_path_data
from parser import parse
from models import code_to_textual
from datetime import datetime
import pprint
import subprocess


def get_bad_lines(bills):
    bad_bill_ranges = [range(bill.start_line, bill.end_line + 1) for bill in bills if any(bill.has_errors)]
    bad_lines = []
    
    for bad_range in bad_bill_ranges:
        bad_range = list(bad_range)
        bad_lines.extend(bad_range)
    # print(bad_lines)
    return set(bad_lines)


def purge_bad_lines(filename, bad_lines, output_route):
    path = get_path_data(filename)
    target_good = f"{output_route}/{path.name}{path.suffix}"
    target_bad = f"{output_route}/{path.name}{path.suffix}.err"
    
    if bad_lines:
        if not os.path.exists(output_route):
            os.mkdir(output_route)
        with open(filename, "r", encoding="ISO-8859-1") as original, \
            open(target_good, "w", encoding="ISO-8859-1") as processed_good, \
            open(target_bad, "w", encoding="ISO-8859-1") as processed_bad:
                
            for index, line in enumerate(original):
                line = f"{line}"
                if index not in bad_lines:
                    processed_good.write(line)
                else:
                    processed_bad.write(line)
    else:
        print("no erros in lines found")


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
            # textual_codes = sorted(code_to_textual(bill.missing_sections))
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
    # from pyinstrument import Profiler
    # parser = argparse.ArgumentParser(
    #     usage="introduce la ruta del archivo con facturas:\n Bills.txt"
    # )
    

    # parser.add_argument("--file", help="Ruta/de/archivo.txt ")
    # parser.add_argument("--output", help="Ruta/para/archivos/procesados")
    # parser.add_argument("--billtype", help="Tipo de parseo a ejecutar: fixed/mobile",  choices=["fixed", "mobile"])

    # args = parser.parse_args()

    # if not args.file  or not args.billtype:
    #     parser.print_help()
    #     exit()

    # if args.output is None:
    #     args.output = "./output/"

    # filename = args.file
    # output_route = args.output
    # parse_type = args.billtype
    
    global_start_time = datetime.now()
    
    path = "/home/vakord/Work/bill_parser/casos/new_cases/"
    dir_list = os.listdir(path)
    for file in dir_list:
        print(f"iniciando validacion para archivo: {file}")
        file_path = path + file
        filename = file_path
        output_route = "../casos/errortests/"
        parse_type = "mobile"

        start_time = datetime.now()
        print("parsing bill...")

        # profiler = Profiler()
        # profiler.start()

        fp = file_stream_reader(filename)
        if parse_type == "fixed": 
            bills = parse(fp, [7,8,30], parse_type=parse_type)
        elif parse_type == "mobile":
            bills = parse(fp, parse_type=parse_type)

        print_errors(bills)
        # print_bill_ranges(bills)

        bad_lines = get_bad_lines(bills)
        print("purging bad lines...")
        purge_bad_lines(filename, bad_lines, output_route)

        end_time = datetime.now()

        # profiler.stop()
        # output = profiler.output_html()
        # print("wrapping up profiler...")
        # fp = open("profile_run.html", "w")
        # fp.write(output)
        # fp.close();
        print(f"validating time: {end_time - start_time}")
    
    global_end_time = datetime.now()
        
    print(f"global validating time: {global_end_time - global_start_time}")

if __name__ == '__main__':
    
    main()
    