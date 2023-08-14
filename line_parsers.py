from functools import partial
from collections import namedtuple
from models import LineError

# Helpers

def undefined_line(line_index, parsed):
    # print(f"Undefined behaviour at l: {line_index} for line id: {parsed.section_index}")
    # print(f"Predicate is: {parsed.predicate}")
    pass

def generic_predicate(bill, line_index, parsed, field_name):
    value = parsed.predicate.strip()
    setattr(bill, field_name, value)


def split_predicate(line_index, parsed, bill, split_len):
    tokens = parsed.predicate.split()
    if len(tokens) < split_len:
        error_msg = f"Could not split line in {split_len}"
        error = LineError(
            line_section_index=parsed.section_index,
            predicate=parsed.predicate,
            line_index=line_index,
            error=error_msg
        )
        bill.errors.append(error)
        return None
    return tokens


# SECTION 010000

def parse_0100000(bill, line_index, parsed):
    # undefined_line(line_index, parsed)
    bill.id = line_index
    bill.segmentacion = parsed.predicate

def parse_0100100(bill, line_index, parsed):
    # undefined_line(line_index, parsed)
    line = parsed.predicate[82:]
    if len(line) == 15:
        line += " " *(29 - 15)
    
    bill.person_id = line[-9:].strip()
    bill.fecha_emision = line[:11]
    bill.imprime_ciclo = line[11:12]
    bill.con_inanciamiento = line[12:13]
    bill.tipoFactura = line[13:14]
    bill.imprimeBPS = line[14:15]
    bill.refactura = 'N'

    

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
    tokens = split_predicate(line_index, parsed, bill, 3)

    if not tokens:
        return

    bill.factura_serie = tokens[2]
    
    if bill.factura_serie == 'NUMERO':
        bill.factura_serie = ' '
    
    bill.numero_autorizacion = tokens[-1]
    if bill.numero_autorizacion == 'AUTORIZACION:':
        bill.numero_autorizacion = ' '
    

def parse_0100700(bill, line_index, parsed):
    tokens = split_predicate(line_index, parsed, bill, 4)
    
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
    undefined_line(line_index, parsed)
    # tokens = split_predicate(line_index, parsed,)

def parse_0100900(bill, line_index, parsed):
    undefined_line(line_index, parsed)

def parse_0101000(bill, line_index, parsed):
    undefined_line(line_index, parsed)

def parse_0101100(bill, line_index, parsed):
    undefined_line(line_index, parsed)

def parse_0101200(bill, line_index, parsed):
    undefined_line(line_index, parsed)

def parse_0101300(bill, line_index, parsed):
    undefined_line(line_index, parsed)
    
def parse_0101400(bill, line_index, parsed):
    undefined_line(line_index, parsed)

def parse_0101500(bill, line_index, parsed):
    undefined_line(line_index, parsed)

def parse_0101600(bill, line_index, parsed):
    undefined_line(line_index, parsed)

def parse_0101700(bill, line_index, parsed):
    undefined_line(line_index, parsed)



# SECTION 010000