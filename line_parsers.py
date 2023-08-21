from datetime import datetime
from functools import partial
from collections import namedtuple
from models import LineError, SECTION_NAME_MAP
from ioutils import file_stream_reader
from helpers import *
import re

# SECTION 010000

def parse_0100000(bill, line_index, parsed):
    # undefined_line(line_index, parsed)
    bill._01_id = parsed.section_index
    bill.segmentacion = parsed.predicate

person_ids_seen = set()
def parse_0100100(bill, line_index, parsed):
    tokens = split_predicate(line_index, parsed, bill, 4)
    expected_patterns = [
        ("EMISION:", "First token is missing or does not match the expected pattern."),
        (r"\d{1,2}/[A-Z]{3}/\d{4}[A-Z]{2}", "Second token is missing or does not match the expected pattern."),
        (['N'], "Third token is missing or does not match the expected pattern."),
        (r"\d{9}", "Fourth token has no person ID."),
    ]

    errors = validate_tokens(tokens, expected_patterns)

    person_id = tokens[3] if tokens and len(tokens) > 3 else None

    if person_id and person_id in person_ids_seen:
        error_message = "Person ID is duplicated."
        append_line_error(bill, parsed, line_index, error_message)
    elif person_id:
        person_ids_seen.add(person_id)
    
    for error_message in errors:
        append_line_error(bill, parsed, line_index, error_message)
    

parse_0100200 = partial(generic_predicate, field_name="emisor_nombre")
parse_0100300 = partial(generic_predicate, field_name="emisor_dir_l1")
parse_0100400 = partial(generic_predicate, field_name="emisor_dir_l2")

def parse_0100500(bill, line_index, parsed):
    tokens = split_predicate(line_index, parsed, bill, 3)
    expected_patterns = [
        (r"\d{5}", "First token is missing or does not match the expected pattern."),
        ("N.I.T.", "Second token is missing or does not match the expected pattern."),
        (r"\d{6}-\d", "Third token is missing or does not match the expected pattern."),
    ]
    errors = validate_tokens(tokens, expected_patterns)
    
    for error_message in errors:
        append_line_error(bill, parsed, line_index, error_message)

def parse_0100600(bill, line_index, parsed):
    tokens = split_predicate(line_index, parsed, bill, 6)

def parse_0100700(bill, line_index, parsed):
    tokens = split_predicate(line_index, parsed, bill, 8)

def parse_0100800(bill, line_index, parsed):
    tokens = parsed.predicate.split()
    if len(tokens) < 7:
        append_line_error(bill, parsed, line_index, "there is not the minimum length for tokens")

parse_0100900 = partial(generic_predicate, field_name="receptor_nombre")

def parse_0101000(bill, line_index, parsed):
    tokens = parsed.predicate[63-7:].split()
    if len(tokens) != 5:
        append_line_error(bill, parsed, line_index, "there is not the required length for tokens")
        
parse_0101100 = partial(generic_predicate, field_name="receptor_dir_l2")

def parse_0101200(bill, line_index, parsed):
    line = parsed.predicate
    line_list= [
        # line[:45-7].strip(),
        line[51-7:71-7].strip(),
        line[73-7:-1].strip(),
    ]
    for item in line_list:
        # print(item)
        if len(item.strip()) == 0:
            append_line_error(bill,parsed,line_index, "missing values in parsed file")

parse_0101300 = partial(generic_predicate, field_name="limite")

def parse_0101400(bill, line_index, parsed):
    undefined_line(line_index, parsed)

parse_0101500 = partial(generic_predicate, field_name="mensaje")

def parse_0101600(bill, line_index, parsed):
    line = parsed.predicate[33:].strip()

parse_0101700 = partial(generic_predicate, field_name="eMails")


# SECTION 020000
def parse_0200000(bill,line_index,parsed):
    undefined_line(line_index, parsed)
        
def parse_0200100(bill,line_index,parsed):
    tokens = parsed.predicate.split()
    token = tokens[-1]
    try:
        datetime.strptime(token, '%d/%b/%Y')
    except ValueError:
        error_msg = "missing billed period"
        append_line_error(bill, parsed, line_index, error_msg)
        
parse_0200200 = partial(generic_predicate, field_name="concepto_cobro")
        
def parse_0200300(bill,line_index,parsed): # SALDO ANTERIORm
    tokens = split_predicate(line_index,parsed,bill,5)
    if len(tokens) != 5:
        append_line_error(bill, parsed, line_index, "length of tokens are not 5")
        
def parse_0200400(bill,line_index,parsed): # SU PAGO GRACIAS
    tokens = split_predicate(line_index,parsed,bill,5)
    if len(tokens) != 5:
        append_line_error(bill, parsed, line_index, "length of tokens are not 5")
    
def parse_0200500(bill,line_index,parsed): # SALDO INICIAL
    tokens = split_predicate(line_index, parsed, bill, 5)
    if len(tokens) != 5:
        append_line_error(bill, parsed, line_index, "length of tokens are not 5")
        
def parse_0200600(bill,line_index,parsed): # CARGOS DEL MES
    tokens = split_predicate(line_index, parsed, bill, 3)
        
def parse_0200700(bill,line_index,parsed): # FINANCIAMIENTO
    tokens = split_predicate(line_index, parsed, bill, 5)
    
        
def parse_0200800(bill,line_index,parsed): # SALDO INICIAL + FINANCIAMIENTO
    tokens = split_predicate(line_index, parsed, bill, 5)
   
    
# SECTION 030000

def parse_0300000(bill,line_index,parsed): # FINANCIAMIENTOS
    undefined_line(line_index, parsed)
    
def parse_0300010(bill,line_index,parsed):
    undefined_line(line_index, parsed)
    # tokens = split_predicate(line_index, parsed, bill, 5)
    
def parse_0300100(bill,line_index,parsed): # PRODUCTOS Y SERVICIOS
    undefined_line(line_index, parsed)
    
def parse_0300110(bill,line_index,parsed):
    
    tokens = split_predicate(line_index, parsed, bill, 7)
    
def parse_0300150(bill,line_index,parsed):
    tokens = split_predicate(line_index, parsed, bill, 5)
    
parse_0302000 = partial(generic_predicate, field_name="aviso")

def parse_0300300(bill,line_index,parsed):
    tokens = split_predicate(line_index, parsed, bill, 5)
    
parse_0401000 = partial(generic_predicate, field_name="notificaciones")    
# def parse_0400100(bill,line_index,parsed):
#     undefined_line(line_index, parsed)

#

parse_0402000 = partial(generic_predicate, field_name="serieAdministrativa") 
# def parse_0400200(bill, line_index, parsed):
#     undefined_line(line_index, parsed)

parse_0403000 = partial(generic_predicate, field_name="numeroAdministrativo") 
# def parse_0400300(bill, line_index, parsed):
#     undefined_line(line_index, parsed)

def parse_0400400(bill, line_index, parsed):
    tokens = split_predicate(line_index,parsed,bill,4)
        
def parse_0400410(bill, line_index, parsed):
    bill.factura_serie = parsed.predicate[21-7:51-7].strip()
    
def parse_0400500(bill, line_index, parsed):
    tokens = split_predicate(line_index,parsed,bill,2)

def parse_0400600(bill, line_index, parsed):
    tokens = split_predicate(line_index,parsed,bill,5)
    # bill.mesFacturacion = tokens.pop()
    
def parse_0400700(bill, line_index, parsed):
    tokens = split_predicate(line_index, parsed, bill, 5)
    
def parse_0400800(bill, line_index, parsed):
    tokens = split_predicate(line_index, parsed, bill, 5)
    
def parse_0401000(bill, line_index, parsed):
    tokens = split_predicate(line_index, parsed, bill, 2)
    
def parse_0401098(bill, line_index, parsed):
    tokens = split_predicate(line_index, parsed, bill, 4)
    
def parse_0401099(bill, line_index, parsed):
    tokens = split_predicate(line_index, parsed, bill, 3)
    
def parse_0401100(bill, line_index, parsed):
    tokens = split_predicate(line_index, parsed, bill, 3)

def parse_0700100(bill, line_index, parsed):
    bill._07_id = parsed.section_index
    
def parse_0800100(bill, line_index, parsed):
    undefined_line(line_index,parsed)
    
def parse_0900100(bill, line_index, parsed):
    tokens = split_predicate(line_index, parsed, bill, 2)
    
def parse_0900200(bill, line_index, parsed):
    tokens = split_predicate(line_index, parsed, bill, 15)
    # bill._0900200_tokens = tokens

def parse_0900300(bill, line_index, parsed):
    # min_len = bill._0900200_tokens
    tokens = split_predicate(line_index, parsed, bill, 15)    

def parse_0900400(bill, line_index, parsed):
    tokens = split_predicate(line_index, parsed, bill, 8)  

def parse_0900500(bill, line_index, parsed):
    tokens = split_predicate(line_index, parsed, bill, 7)
    
def parse_1100100(bill, line_index, parsed):
    tokens = split_predicate(line_index, parsed, bill, 3)

def parse_1100200(bill, line_index, parsed):
    # tokens = parsed.predicate.split()
    tokens = split_predicate(line_index, parsed, bill, 12)
    
def parse_1100300(bill, line_index, parsed):
    undefined_line(line_index, parsed)
    
def parse_1100400(bill, line_index, parsed):
    undefined_line(line_index,parsed)

    
# def parse_11_00300(bill, line_index, parsed):
#     undefined_line(line_index, parsed)

# def parse_09_11_00400(bill, line_index, parsed):
#     undefined_line(line_index, parsed)
    
# def parse_09_11_00500(bill, line_index, parsed):
#     undefined_line(line_index, parsed)
    
def parse_1201100(bill, line_index, parsed):
    undefined_line(line_index, parsed)
    
def parse_1201200(bill, line_index, parsed):
    undefined_line(line_index, parsed)
    
def parse_1201300(bill, line_index, parsed):
    undefined_line(line_index, parsed)
    
def parse_1201400(bill, line_index, parsed):
    undefined_line(line_index, parsed)
    
def parse_1300100(bill, line_index, parsed):
    undefined_line(line_index, parsed)
    
def parse_1300200(bill, line_index, parsed):
    undefined_line(line_index, parsed)
    
def parse_1300300(bill, line_index, parsed):
    undefined_line(line_index, parsed)
    
def parse_1300400(bill, line_index, parsed):
    undefined_line(line_index, parsed)
    
    
def parse_1400100(bill, line_index, parsed):
    undefined_line(line_index,parsed)