#!/usr/bin/env python
from constants import (
    HEAD_SLICE,
    PREDICATE_SLICE,
    SECTION_SLICE,
    SUB_SECTION_SLICE,
    SUB_SUB_SECTION_SLICE,
)

class Bill:
    def __init__(self, section_index, section, sub_section, sub_sub_section, predicate):
        self.section_index = section_index
        self.section = section
        self.sub_section = sub_section
        self.sub_sub_section = sub_sub_section
        self.predicate = predicate
    def __str__(self):
        return (
            f"Section Index: {self.section_index}\n"
            f"Section: {self.section}\n"
            f"Sub-Section: {self.sub_section}\n"
            f"Sub-Sub-Section: {self.sub_sub_section}\n"
            f"Predicate: {self.predicate}"
        )
    # self.nombre: str
    # self.fecha: datetime
    # self.cargos = []
    
    # def parse_cargos(self, line):
    #     cargo = line[3:3]
    #     concepto = line[5: 10]
    #     cargos.append()


def parse(fp):
    bills = []
    for index, line in enumerate(fp):
        parsed = tokenize(index, line)
        bills.append(parsed)
    return bills

def tokenize(index, line):
    section_index = line[HEAD_SLICE]
    payload = {
        "section_index": section_index,
        "section": section_index[SECTION_SLICE],
        "sub_section": section_index[SUB_SECTION_SLICE],
        "sub_sub_section": section_index[SUB_SUB_SECTION_SLICE],
        "predicate": line[PREDICATE_SLICE],
    }
    return payload

def segment_bills(lines):
    bills = []
    current_bill = []
    inside_bill = False
    
    for line in lines:
        section_index = line.get('section_index')
        # section_index = line['section_index']
        
        if section_index == '0100000':
            inside_bill = True
            current_bill = []
        
        if inside_bill:
            current_bill.append(line)
        
        if section_index == '5000100':
            if inside_bill:
                bills.append(current_bill)
                inside_bill = False
    
    return bills