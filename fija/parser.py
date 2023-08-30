#!/usr/bin/env python

from ioutils import file_stream_reader
from collections import namedtuple
from helpers import append_line_error
from constants import (
    HEAD_SLICE,
    PREDICATE_SLICE,
    SECTION_SLICE,
    SUB_SECTION_SLICE,
    SUB_SUB_SECTION_SLICE,
)
from models import Bill, Line
import line_parsers
from models import LineError, SECTION_NAME_MAP

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
        # error = LineError(
        #     line_section_index=line_index,
        #     section_name=SECTION_NAME_MAP.get(parsed.section_index, "Unknown"),
        #     predicate="Missing",
        #     line_index=line_index,
        #     error="Section not found"
        # )
        # bill.errors.append(error)
        # bill.processed_lines.append(parsed.section_index)
        # return  
        # raise KeyError(f"Line code { parsed.section_index} is missing from line parsers ")
        # return
        bill.processed_lines.append(parsed.section_index)
        return
    func(bill, line_index, parsed)
    bill.processed_lines.append(parsed.section_index)


def parse(iterable, excluded_sections=None):
    if excluded_sections is None:
        excluded_sections = []
        
    bills = []
    bill = Bill(start_line=0)
    
    prev_section = 1

    for index, line in enumerate(iterable):
        line_index = index
        parsed = tokenize(line)
        curr_section = int(parsed.section)
        
        if (
            (curr_section == 50 or 
            (curr_section < prev_section and curr_section not in excluded_sections))
        ):

            load_line(bill, line_index, parsed)
            bill.end_line = index
            bills.append(bill)
            bill = Bill(start_line=index + 1)
            prev_section = 1
            continue
        
        load_line(bill, line_index, parsed)
        prev_section = int(parsed.section)
        
    if curr_section != 50: # stray bill
        bill.end_line = index + 1
        bills.append(bill)
    return bills
