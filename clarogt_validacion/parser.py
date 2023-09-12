#!/usr/bin/env python

from ioutils import file_stream_reader
from collections import namedtuple
from helpers import append_line_error
from constants import (
    FIXED_HEAD_SLICE,
    FIXED_PREDICATE_SLICE,
    FIXED_SECTION_SLICE,
    FIXED_SUB_SECTION_SLICE,
    FIXED_SUB_SUB_SECTION_SLICE,
    
    MOBILE_HEAD_SLICE,
    MOBILE_SECTION_SLICE,
    MOBILE_SUB_SECTION_SLICE,
    MOBILE_SUB_SUB_SECTION_SLICE,
    MOBILE_PREDICATE_SLICE,
)

from models import FixedBill, MobileBill, Line
import line_parsers_fixed
import line_parsers_mobile
from models import LineError

def tokenize_fixed(line):
    section_index = line[FIXED_HEAD_SLICE]
    line  = Line(
        section_index=section_index,
        section=section_index[FIXED_SECTION_SLICE],
        sub_section=section_index[FIXED_SUB_SECTION_SLICE],
        sub_sub_section=section_index[FIXED_SUB_SUB_SECTION_SLICE],
        predicate=line[FIXED_PREDICATE_SLICE],
    )
    return line


def tokenize_mobile(line):
    section_index = line[MOBILE_HEAD_SLICE]
    line  = Line(
        section_index=section_index,
        section=section_index[MOBILE_SECTION_SLICE],
        sub_section=section_index[MOBILE_SUB_SECTION_SLICE],
        sub_sub_section=section_index[MOBILE_SUB_SUB_SECTION_SLICE],
        predicate=line[MOBILE_PREDICATE_SLICE],
    )
    return line



def load_line(bill, line_index, parsed, parse_type):

    if parse_type == "fixed":
        target = line_parsers_fixed
    else:
        target = line_parsers_mobile
    
    func = getattr(target, f"parse_{parsed.section_index}", None)
    
    # Guard class in case of missing section
    if func is None:
        # error = LineError(
        #     line_section_index=line_index,
        #     section_name=bill.SECTION_NAME_MAP.get(parsed.section_index, "Unknown"),
        #     predicate="Missing",
        #     line_index=line_index,
        #     error="Section not found"
        # )
        # bill.errors.append(error)
        bill.processed_lines.append(parsed.section_index)
        return 

    func(bill, line_index, parsed)
    bill.processed_lines.append(parsed.section_index)


def parse(iterable, excluded_sections=None, parse_type=None):
    
    if parse_type == "fixed":
        BillModel = FixedBill
    elif parse_type == "mobile":
        BillModel = MobileBill
    else:
        raise ValueError(f"Invalid parse type '{parse_type}'")
    
    if excluded_sections is None:
        excluded_sections = []
        
    bills = []
    bill = BillModel(start_line=0)
    
    prev_section = 1

    for index, line in enumerate(iterable):
        line_index = index
        
        if parse_type == "fixed":
            parsed = tokenize_fixed(line)
        
        else:
            parsed = tokenize_mobile(line)

        curr_section = int(parsed.section)
        
        if (
            (curr_section == 50 or 
            (curr_section < prev_section and curr_section not in excluded_sections))
        ):

            load_line(bill, line_index, parsed, parse_type)
            bill.end_line = index
            bills.append(bill)
            bill = BillModel(start_line=index + 1)
            prev_section = 1
            continue
        
        load_line(bill, line_index, parsed, parse_type)
        prev_section = int(parsed.section)
        
    if curr_section != 50: # stray bill
        bill.end_line = index + 1
        bills.append(bill)
    return bills
