#!/usr/bin/env python

from ioutils import file_stream_reader
from collections import namedtuple
from constants import (
    HEAD_SLICE,
    PREDICATE_SLICE,
    SECTION_SLICE,
    SUB_SECTION_SLICE,
    SUB_SUB_SECTION_SLICE,
)
from models import Bill, Line
import line_parsers

def tokenize(line):
    section_index = line[HEAD_SLICE]
    line  = Line(
        section_index=section_index,
        section=section_index[SECTION_SLICE],
        sub_section=section_index[SUB_SECTION_SLICE],
        sub_sub_section=section_index[SUB_SUB_SECTION_SLICE],
        predicate=line[PREDICATE_SLICE],
    )
    return line


def load_line(bill, line_index, parsed):
    func = getattr(line_parsers, f"parse_{parsed.section_index}", None)
    
    # Guard class in case of missing section
    if func is None:
        # raise KeyError(f"Line code { parsed.section_index} is missing from line parsers ")
        # return
        bill.processed_lines.append(parsed.section_index)
        return

    func(bill, line_index, parsed)
    bill.processed_lines.append(parsed.section_index)


def parse(iterable):
    bills = []
    bill = Bill(starting_line_human=1, start_line_real=0)
    
    prev_section = 1

    for index, line in enumerate(iterable):
        line_index = index + 1
        parsed = tokenize(line)
        curr_section = int(parsed.section)
        
        if curr_section == 50 or curr_section < prev_section:

            load_line(bill, line_index, parsed)
            bill.ending_line=index
            bills.append(bill)
            bill = Bill(starting_line_human=index + 2, start_line_real=index + 1)
            prev_section = 1
            continue
        
        load_line(bill, line_index, parsed)
        prev_section = int(parsed.section)
        
    if curr_section != 50: # stray bill
        bills.append(bill)
    return bills
