#!/usr/bin/env python
from ioutils import file_stream_reader 
from parser import parse, segment_bills
import pprint

def get_parameters():
    params = {
        "filename": "ClaroGT/ClaroGT_TelefoniaFija/TelefoniaFija/20230502123330_CLAROGTFIJA_GFTX230311_ver01.txt.err"
    }
    return params

def main():
    params = get_parameters()
        
    fp = file_stream_reader(params["filename"])
    payloads = parse(fp)
    segmented_bills = segment_bills(payloads)

    bills = segmented_bills
    bill = bills[0]
    # print(bill)
    bill.insert(0, bill)
    pprint.pprint(bill)

if __name__ == '__main__':
    main()

