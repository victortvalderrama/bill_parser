from functools import partial
from collections import namedtuple
from models import LineError, SECTION_NAME_MAP
from ioutils import *

# SECTION 010000

def parse_0100000(bill, line_index, parsed):
    # undefined_line(line_index, parsed)
    bill._01_id = parsed.section_index
    bill.segmentacion = parsed.predicate

def parse_0100100(bill, line_index, parsed):
    line = parsed.predicate
    
    # if len(line) == 15:
    #     line += " " *(29 - 15)
    
    # bill.person_id = line[-9:].strip()
    # bill.fecha_emision = line[:11]
    # bill.imprime_ciclo = line[11:12]
    # bill.con_inanciamiento = line[12:13]
    # bill.tipoFactura = line[13:14]
    # bill.imprimeBPS = line[14:15]
    # bill.refactura = 'N'

    

parse_0100200 = partial(generic_predicate, field_name="emisor_nombre")
parse_0100300 = partial(generic_predicate, field_name="emisor_dir_l1")
parse_0100400 = partial(generic_predicate, field_name="emisor_dir_l2")

def parse_0100500(bill, line_index, parsed):
    tokens = split_predicate(line_index, parsed, bill, 3)
    
    if not tokens:
        return

    bill.emisor_dir_l3 = tokens[0]
    bill.emisor_NIT = tokens[2]

def parse_0100600(bill, line_index, parsed):
    tokens = split_predicate(line_index, parsed, bill, 6)

    if not tokens:
        return

    bill.factura_serie = tokens[2]
    
    if bill.factura_serie == 'NUMERO':
        bill.factura_serie = ' '
    
    bill.numero_autorizacion = tokens[-1]
    if bill.numero_autorizacion == 'AUTORIZACION:':
        bill.numero_autorizacion = ' '
    

def parse_0100700(bill, line_index, parsed):
    tokens = split_predicate(line_index, parsed, bill, 8)
    
    if not tokens:
        return
    
    bill.documento_numero = tokens[1]

    if bill.documento_numero == 'PARA':
        bill.documento_numero = ' '
        bill.documento_numero_V2 = bill.documento_numero.replace(',', '')
        bill.consultas = " ".join(tokens[1:-1])
    
    else:
        bill.documento_numero_V2 = bill.documento_numero.replace(',', '')
        bill.consultas = " ".join(tokens[2:-1])


def parse_0100800(bill, line_index, parsed):
    tokens = parsed.predicate.split()
    
    if not tokens:
        return
    
    refTokens = tokens[1].split('//')
    # print(refTokens)
    bill.ciclo = refTokens[0]
    bill.correlativo = refTokens[1]
    if len(tokens) == 8:
        bill.rutaCourier = tokens[2]
    else:
        bill.rutaCourier = ' '

parse_0100900 = partial(generic_predicate, field_name="receptor_nombre")

def parse_0101000(bill, line_index, parsed):
    # undefined_line(line_index, parsed)
    bill.receptorDirL1 = parsed.predicate[0:47].strip()
    bill.receptorTelefono = parsed.predicate[47:57].strip()

    data_total_pagar = parsed.predicate[57:78].strip()
    if data_total_pagar.startswith('Q-'):
        bill.totalPagar = "-" + data_total_pagar.split()[1]
    else:
        bill.totalPagar = data_total_pagar.split()[1]

    if len(parsed.predicate) == 89:
        bill.fechaVencimiento = parsed.predicate[78:89].strip()
    elif len(parsed.predicate) >= 88:
        bill.fechaVencimiento = parsed.predicate[78:90].strip()
    else:
        bill.fechaVencimiento = '01/ENE/1970'

    if len(parsed.predicate) >= 113:
        data_ajuste = parsed.predicate[95:].strip()
        if data_ajuste.startswith('Q-'):
            bill.ajuste = "-" + data_ajuste.split()[1]
        else:
            bill.ajuste = data_ajuste.split()[1]
    else:
        bill.ajuste = '0.00'
        
parse_0101100 = partial(generic_predicate, field_name="receptor_dir_l2")

def parse_0101200(bill, line_index, parsed):
    line = parsed.predicate
    bill.receptorDirL3 = line[0:39].strip()
    bill.receptorNIT = line[44:65].strip()
    bill.servicio = line[72:].strip()
    
parse_0101300 = partial(generic_predicate, field_name="limite")

def parse_0101400(bill, line_index, parsed):
    undefined_line(line_index, parsed)

parse_0101500 = partial(generic_predicate, field_name="mensaje")

def parse_0101600(bill, line_index, parsed):
    line = parsed.predicate
    bill.mensaje += line[33:].strip()

parse_0101700 = partial(generic_predicate, field_name="eMails")


# SECTION 020000


def parse_0200000(bill,line_index,parsed):
    # undefined_line(line_index, parsed)
    bill._02_id = parsed.section_index
        
def parse_0200100(bill,line_index,parsed):
    # 0200100POR VENTA DE SERVICIO TELEFONICO AL:  10/FEB/2021
    # undefined_line(line_index, parsed)
    tokens = parsed.predicate.split()
    bill.periodoFacturado = tokens[-1]
        
parse_0200200 = partial(generic_predicate, field_name="concepto_cobro")
# def parse_0200200(bill,line_index,parsed):
#     # 0200200RESUMEN CONCEPTO DE COBRO
#     #bill.conceptosCobro = [] # new List
#     undefined_line(line_index, parsed)
        
def parse_0200300(bill,line_index,parsed): # SALDO ANTERIORm
    tokens = split_predicate(line_index,parsed,bill,5)
    
        
def parse_0200400(bill,line_index,parsed): # SU PAGO GRACIAS
    tokens = split_predicate(line_index,parsed,bill,5)
    
        
def parse_0200500(bill,line_index,parsed): # SALDO INICIAL
    tokens = split_predicate(line_index, parsed, bill, 5)
    
        
def parse_0200600(bill,line_index,parsed): # CARGOS DEL MES
    tokens = split_predicate(line_index, parsed, bill, 3)
    bill. cobro_section_nombre = " ".join(tokens)
    # bill.conceptosCobro << conceptoCobrobill
        
def parse_0200700(bill,line_index,parsed): # FINANCIAMIENTO
    tokens = split_predicate(line_index, parsed, bill, 5)
    
        
def parse_0200800(bill,line_index,parsed): # SALDO INICIAL + FINANCIAMIENTO
    tokens = split_predicate(line_index, parsed, bill, 5)
   
    
# SECTION 030000

def parse_0300000(bill,line_index,parsed): # FINANCIAMIENTOS
    undefined_line(line_index, parsed)
    
def parse_0300010(bill,line_index,parsed):
    tokens = parsed.predicate.split()
    
def parse_0300100(bill,line_index,parsed): # PRODUCTOS Y SERVICIOS
    # sectionRecord.productosServicios = []
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