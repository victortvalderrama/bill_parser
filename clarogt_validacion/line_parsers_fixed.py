from datetime import datetime
from functools import partial
from collections import namedtuple
from models import LineError
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
        (re.compile(r"\d{1,2}/[A-Z]{3}/\d{4}[A-Z]{2}"), "Second token is missing or does not match the expected pattern."),
        ('N', "Third token is missing or does not match the expected pattern."),
        (re.compile(r".*(\d{9})$"), "Invalid person ID."),
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
        (re.compile(r"\d{5}"), "First token is missing or does not match the expected pattern."),
        ("N.I.T.", "Second token is missing or does not match the expected pattern."),
        (re.compile(r"\d{6}-\d"), "Third token is missing or does not match the expected pattern."),
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
    if len(tokens) > 6:
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
        if len(item.strip()) == 0:
            append_line_error(bill,parsed,line_index, "missing values in parsed file")

parse_0101300 = partial(generic_predicate, field_name="limite")

def parse_0101400(bill, line_index, parsed):
    undefined_line(line_index, parsed)

parse_0101500 = partial(generic_predicate, field_name="mensaje")

parse_0101600 = partial(generic_predicate, field_name="mensaje2")

parse_0101700 = partial(generic_predicate, field_name="eMails")

# SECTION 020000
def parse_0200000(bill,line_index,parsed):
    undefined_line(line_index, parsed)
        
def parse_0200100(bill,line_index,parsed):
    tokens = parsed.predicate.split()
    token = tokens[-1]
    pattern = r"\d{2}/[A-Z]{3}/\d{4}$"
    if not re.match(pattern, token):
        append_line_error(bill, parsed, line_index, "missing billed period")
        
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
        
parse_0200600 = partial(generic_predicate, field_name="cargos_del_mes")
        
def parse_0200700(bill,line_index,parsed): # FINANCIAMIENTO
    tokens = split_predicate(line_index, parsed, bill, 5)
    
        
def parse_0200800(bill,line_index,parsed): # SALDO INICIAL + FINANCIAMIENTO
    tokens = split_predicate(line_index, parsed, bill, 6)
   
    
# SECTION 030000
# parse_0300000 = partial(generic_predicate, field_name="productos_y_servicios")
def parse_0300000(bill,line_index,parsed): # FINANCIAMIENTOS
    undefined_line(line_index, parsed)
    
def parse_0300010(bill,line_index,parsed):
    undefined_line(line_index, parsed)
    # tokens = split_predicate(line_index, parsed, bill, 5)
  
# parse_0300100 = partial(generic_predicate, field_name="productos_y_servicios")  
def parse_0300100(bill,line_index,parsed): # PRODUCTOS Y SERVICIOS
    undefined_line(line_index, parsed)
    
def parse_0300110(bill,line_index,parsed):
    tokens = parsed.predicate[63-7:].split()
    if len(tokens) > 4:
        append_line_error(bill, parsed, line_index, "length of tokens are not 4")
    
def parse_0300150(bill,line_index,parsed):
    tokens = parsed.predicate[57-7:].split()
    if len(tokens) != 2:
        append_line_error(bill, parsed, line_index, "length of tokens are not 2")
    
parse_0302000 = partial(generic_predicate, field_name="aviso")

def parse_0300300(bill,line_index,parsed):
    tokens = parsed.predicate[57-7:].split()
    if len(tokens) != 2:
        append_line_error(bill, parsed, line_index, "length of tokens are not 2")
    
parse_0401000 = partial(generic_predicate, field_name="notificaciones")    

def parse_0400200(bill, line_index, parsed):
    tokens = split_predicate(line_index, parsed, bill, 3)

def parse_0400300(bill, line_index, parsed):
    tokens = split_predicate(line_index, parsed, bill, 5)
    if len(tokens) < 5:
        append_line_error(bill,parsed,line_index, "no tokens enough")

def parse_0400400(bill, line_index, parsed):
    tokens = split_predicate(line_index,parsed,bill,4)
    # if len(tokens) < 4:
    #     append_line_error(bill,parsed,line_index, "no authorization logic")
        
# parse_0400410(generic_predicate, field_name="numero_autorizacion")
def parse_0400410(bill, line_index, parsed):
    undefined_line(line_index, parsed)

def parse_0400500(bill, line_index, parsed):
    tokens = split_predicate(line_index,parsed,bill,2)

def parse_0400600(bill, line_index, parsed):
    tokens = split_predicate(line_index,parsed,bill,4)
    # bill.mesFacturacion = tokens.pop()
    
def parse_0400700(bill, line_index, parsed):
    tokens = split_predicate(line_index, parsed, bill, 5)
    
def parse_0400800(bill, line_index, parsed):
    tokens = split_predicate(line_index, parsed, bill, 5)
    
def parse_0401000(bill, line_index, parsed):
    tokens = split_predicate(line_index, parsed, bill, 2)
    
def parse_0401098(bill, line_index, parsed):
    pattern = r"NIT:\d{7}-\d"
    tokens = parsed.predicate.split()
    if not re.match(pattern, tokens[-1]):
        append_line_error(bill, parsed, line_index, "incorrect nit pattern")
    
def parse_0401099(bill, line_index, parsed):
    tokens = split_predicate(line_index, parsed, bill, 3)
    
def parse_0401100(bill, line_index, parsed):
    tokens = split_predicate(line_index, parsed, bill, 3)
    
def parse_0600100(bill,line_index,parsed):
    tokens = split_predicate(line_index, parsed, bill, 4)

def parse_0600200(bill,line_index,parsed):
    tokens = split_predicate(line_index, parsed, bill, 2)

def parse_0600300(bill,line_index,parsed):
    undefined_line(line_index, parsed)

def parse_0700100(bill, line_index, parsed):
    if bill.first_0800100_encountered and parsed.section == "07":
        append_line_error(bill, parsed, line_index, "0700100 found after the first occurrence of 0800100")
    
def parse_0800100(bill, line_index, parsed):
    bill.first_0800100_encountered = True
    phone = split_predicate(line_index, parsed, bill, 1)
    # tokens = split_predicate(line_index, parsed, bill, 1)
    
def parse_0900100(bill, line_index, parsed):
    tokens = split_predicate(line_index, parsed, bill, 2)
    
def parse_0900200(bill, line_index, parsed):
    tokens = parsed.predicate.split()
    if len(tokens) != 15:
        append_line_error(bill,parsed,line_index,"length of tokens are not 15")

def parse_0900300(bill, line_index, parsed):
    tokens = split_predicate(line_index, parsed, bill, 15)
    if len(tokens) % 5 != 0:
        append_line_error(bill, parsed, line_index, "cant divide length of tokens by 5")

def parse_0900400(bill, line_index, parsed):
    tokens = split_predicate(line_index, parsed, bill, 8)  

def parse_0900500(bill, line_index, parsed):
    tokens = split_predicate(line_index, parsed, bill, 7)
    
def parse_1100100(bill, line_index, parsed):
    tokens = split_predicate(line_index, parsed, bill, 3)
    

def parse_1100200(bill, line_index, parsed):
    tokens = split_predicate(line_index, parsed, bill, 12)
    
def parse_1100300(bill, line_index, parsed):
    range_list = [(30,50),(90,112)]
    tokens = remove_string_segments(parsed.predicate, range_list)
    if len(tokens) % 5 != 0:
        print(tokens)
        append_line_error(bill, parsed, line_index, "invalid number of rows, can't divide by 5") 
    
def parse_1100400(bill, line_index, parsed):
    tokens = split_predicate(line_index, parsed, bill, 10)
    
def parse_1100500(bill, line_index, parsed):
    undefined_line(line_index, parsed)
    
def parse_1201100(bill, line_index, parsed):
    undefined_line(line_index, parsed)
    
def parse_1201200(bill, line_index, parsed):
    undefined_line(line_index, parsed)
    
def parse_1201300(bill, line_index, parsed):
    undefined_line(line_index, parsed)
    
def parse_1201400(bill, line_index, parsed):
    undefined_line(line_index, parsed)
    
def parse_1300100(bill, line_index, parsed):
    tokens = split_predicate(line_index, parsed, bill, 2)
    
def parse_1300200(bill, line_index, parsed):
    tokens = split_predicate(line_index, parsed, bill, 8)
    if len(tokens) % 2 != 0:
        append_line_error(bill, parsed, line_index, "cant divide length of tokens by 2")
    
def parse_1300300(bill, line_index, parsed):
    range_list = [(19,45),(82,108)]
    tokens = remove_string_segments(parsed.predicate, range_list)
    if len(tokens) % 3 != 0:
        print(tokens)
        append_line_error(bill, parsed, line_index, "cant divide length of tokens by 3")
    
parse_1300400 = partial(generic_predicate, field_name="total_otros_servicios")
parse_1400100 = partial(generic_predicate, field_name="resumen_integrados")

def parse_1400200(bill, line_index, parsed):
    tokens = split_predicate(line_index, parsed, bill, 12)

def parse_1400300(bill, line_index, parsed):
    tokens = parsed.predicate.split()
    if len(tokens) != 10:
        append_line_error(bill,parsed,line_index,"invalid token size")
        

def parse_1400500(bill, line_index, parsed):
    tokens = parsed.predicate.split()
    if len(tokens) != 10:
        append_line_error(bill,parsed,line_index,"invalid token size")

def parse_1400600(bill, line_index, parsed):
    tokens = parsed.predicate.split()
    if len(tokens) != 4:
        append_line_error(bill,parsed,line_index,"invalid token size")
        
parse_1401100 = partial(generic_predicate, field_name="detalle_cargos_id")

parse_1401200 = partial(generic_predicate, field_name="detalle_cargos_nombre")

parse_1401210 = partial(generic_predicate, field_name="detalle_cargos_subnombre")

def parse_1401300(bill, line_index, parsed):
    tokens = split_predicate(line_index, parsed, bill, 8)
    
def parse_1401310(bill, line_index, parsed):
    tokens = split_predicate(line_index, parsed, bill, 4)
    
def parse_1401500(bill, line_index, parsed):
    range_list = [(10,25)]
    tokens = remove_string_segments(parsed.predicate, range_list)
    # print(len(tokens))
    if len(tokens) != 7:
        print(len(tokens))
        print(tokens)
        append_line_error(bill,parsed,line_index,"not 7 tokens")
    # if len(tokens) % 2 != 0:
    #     append_line_error(bill, parsed, line_index, "cant divide length of tokens by 2")

def parse_1401600(bill, line_index, parsed):
    tokens = split_predicate(line_index, parsed, bill, 5)
    

# 1800100
parse_1800100 = partial(generic_predicate, field_name="detalle_enlace_id")
parse_1800200 = partial(generic_predicate, field_name="detalle_enlace_fecha")
parse_1800300 = partial(generic_predicate, field_name="detalle_enlace_1800300")

def parse_1800400(bill, line_index, parsed):
    tokens = split_predicate(line_index, parsed, bill, 9)
    
def parse_1800410(bill, line_index, parsed):
    tokens = split_predicate(line_index, parsed, bill, 2)
    
def parse_1800500(bill, line_index, parsed):
    undefined_line(line_index,parsed)
    
    
# parse_1800510 = partial(generic_predicate, field_name="dir1800510")
    
parse_1800600 = partial(generic_predicate, field_name="dir1800600")
    
# 1600100

parse_1600100 = partial(generic_predicate, field_name="detalle_financiamientos_id")

def parse_1600200(bill, line_index, parsed):
    tokens = split_predicate(line_index, parsed, bill, 7)
    
def parse_1600300(bill, line_index, parsed):
    tokens = parsed.predicate[76:].split() 
    if len(tokens) != 7:
        print(tokens)
        append_line_error(bill, parsed, line_index, "invalid number of tokens")

def parse_1600400(bill, line_index, parsed):
    tokens = parsed.predicate[76:].split()
    if len(tokens) != 6:
        append_line_error(bill, parsed, line_index, "invalid number of tokens")
    
def parse_1600500(bill, line_index, parsed):
    tokens = parsed.predicate[102:].split()
    if len(tokens) != 2:
        append_line_error(bill, parsed, line_index, "invalid number of tokens")
    
# 3000000

parse_3000000 = partial(generic_predicate, field_name="300000_id")

def parse_3000100(bill,line_index,parsed):
    tokens = split_predicate(line_index, parsed, bill, 4)
    if len(tokens) < 4:
        append_line_error(bill, parsed, line_index, "invalid number of tokens")
        
parse_3000210 = partial(generic_predicate, field_name="3000210_cliente")
parse_3000220 = partial(generic_predicate, field_name="3000220_cliente_dir")
parse_3000230 = partial(generic_predicate, field_name="3000230_cliente_dir2")
parse_3000240 = partial(generic_predicate, field_name="3000240_cliente_dir3")
parse_3000250 = partial(generic_predicate, field_name="3000250_datoY")
parse_3000260 = partial(generic_predicate, field_name="3000260_cliente_telefono")

def parse_3000300(bill,line_index,parsed):
    tokens = split_predicate(line_index, parsed, bill, 6)

def parse_3000410(bill,line_index,parsed):
    pattern = r"NIT:\d{7}-\d"
    tokens = parsed.predicate.split()
    if not re.match(pattern, tokens[-1]):
        append_line_error(bill, parsed, line_index, "incorrect nit pattern")

def parse_3000420(bill,line_index,parsed):
    tokens = split_predicate(line_index, parsed, bill, 3)

def parse_3000430(bill,line_index,parsed):
    tokens = split_predicate(line_index, parsed, bill, 3)

def parse_3000500(bill,line_index,parsed):
    undefined_line(line_index, parsed)

def parse_3000510(bill,line_index,parsed):
    tokens = parsed.predicate[32:].split()
    if len(tokens) != 2:
        append_line_error(bill, parsed, line_index, "invalid number of tokens")

def parse_3000600(bill,line_index,parsed):
    tokens = parsed.predicate[32:].split()
    if len(tokens) != 2:
        append_line_error(bill, parsed, line_index, "invalid number of tokens")

parse_3000610 = partial(generic_predicate, field_name="3000610_notasAbonado")

parse_3000700 = partial(generic_predicate, field_name="3000700_motivo")
    
# 3200000

parse_3200000 = partial(generic_predicate, field_name="320000_id")

def parse_3200100(bill,line_index,parsed):
    tokens = split_predicate(line_index, parsed, bill, 4)
    if len(tokens) < 4:
        append_line_error(bill, parsed, line_index, "invalid number of tokens")
        
parse_3200210 = partial(generic_predicate, field_name="3200210_cliente")
parse_3200220 = partial(generic_predicate, field_name="3200220_cliente_dir")
parse_3200232 = partial(generic_predicate, field_name="3200232_cliente_dir2")
parse_3200240 = partial(generic_predicate, field_name="3200240_cliente_dir3")
parse_3200250 = partial(generic_predicate, field_name="3200250_datoY")
parse_3200260 = partial(generic_predicate, field_name="3200260_cliente_telefono")

def parse_3200320(bill,line_index,parsed):
    tokens = split_predicate(line_index, parsed, bill, 6)

def parse_3200410(bill,line_index,parsed):
    pattern = r"NIT:\d{7}-\d"
    tokens = parsed.predicate.split()
    if not re.match(pattern, tokens[-1]):
        append_line_error(bill, parsed, line_index, "incorrect nit pattern")

def parse_3200420(bill,line_index,parsed):
    tokens = split_predicate(line_index, parsed, bill, 3)

def parse_3200432(bill,line_index,parsed):
    tokens = split_predicate(line_index, parsed, bill, 3)

def parse_3200500(bill,line_index,parsed):
    undefined_line(line_index, parsed)

def parse_3200510(bill,line_index,parsed):
    tokens = parsed.predicate[32:].split()
    if len(tokens) != 2:
        append_line_error(bill, parsed, line_index, "invalid number of tokens")

def parse_3200600(bill,line_index,parsed):
    tokens = parsed.predicate[32:].split()
    if len(tokens) != 2:
        append_line_error(bill, parsed, line_index, "invalid number of tokens")

parse_3200610 = partial(generic_predicate, field_name="3200610_notasAbonado")

parse_3200700 = partial(generic_predicate, field_name="3200700_motivo")
    