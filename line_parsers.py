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


def split_predicate(line_index, parsed, split_len):
    tokens = parsed.predicate.split()
    if len(tokens) < split_len:
        error_msg = f"Could not split line in {split_len}"
        error = LineError(
            line_section_index=parsed.section_index,
            predicate=parsed.predicate,
            line_index=line_index,
            error=error_msg
        )
        return error
    return tokens


# SECTION 010000

def parse_0100000(bill, line_index, parsed):
    undefined_line(line_index, parsed)

def parse_0100100(bill, line_index, parsed):
    undefined_line(line_index, parsed)

parse_0100200 = partial(generic_predicate, field_name="emisor_nombre")
parse_0100300 = partial(generic_predicate, field_name="emisor_dir_l1")
parse_0100400 = partial(generic_predicate, field_name="emisor_dir_l2")

def parse_0100500(bill, line_index, parsed):
    result = split_predicate(line_index, parsed, 3)
    
    if type(result) != list:
        bill.errors.append(result)

    bill.emisor_dir_l3 = tokens[0]
    bill.emisor_NIT = tokens[2]

def parse_0100600(bill, line_index, parsed):
    result = split_predicate(line_index, parsed, 3)
    
    if type(result) != list:
        bill.errors.append(result)

    bill.factura_serie = result[2]
    
    if sectionRecord.facturaSerie == 'NUMERO':
        sectionRecord.facturaSerie = ' '
    
    sectionRecord.numeroAutorizacion = tokens[-1]
    if sectionRecord.numeroAutorizacion == 'AUTORIZACION:':
        sectionRecord.numeroAutorizacion = ' '
    

def parse_0100700(bill, line_index, parsed):
    result = split_predicate(line_index, parsed, 4)
    
    if type(result) != list:
        bill.errors.append(result)
    
    sectionRecord.documentoNumero = tokens[1]

    if sectionRecord.documentoNumero == 'PARA':
        sectionRecord.documentoNumero = ' '
        sectionRecord.documentoNumeroV2 = sectionRecord.documentoNumero.replace(',', '')
        sectionRecord.consultas = " ".join(tokens[1:-1])
    
    else:
        sectionRecord.documentoNumeroV2 = sectionRecord.documentoNumero.replace(',', '')
        sectionRecord.consultas = " ".join(tokens[2:-1])


def parse_0100800(bill, line_index, parsed):
    undefined_line(line_index, parsed)

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