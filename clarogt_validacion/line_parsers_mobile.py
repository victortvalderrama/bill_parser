from datetime import datetime
from functools import partial
from collections import namedtuple
from models import LineError
from ioutils import file_stream_reader
from helpers import *
import re

def maximum_mobile_tokens(line_index, parsed, bill, split_len):
    string = parsed.predicate[28:]
    tokens = string.split()
    if len(tokens) > split_len:
        append_line_error(bill, parsed, line_index, "exceeded tokens")
        return None
    return  tokens

def extract_sections(input_string, ranges):
    sections = []
    for start, end in ranges:
        section = input_string[start:end]
        sections.append(section)
    return sections

def parse_by_consumption_detail(line_index, parsed, bill, range_list, divide):
    pipe_removed = parsed.predicate.replace("|", " ")
    detail = bill._300102_predicate.strip()
    tokens = remove_string_segments(pipe_removed, range_list)
    extracted = extract_sections(pipe_removed, range_list)
    for extract in extracted:
        if extract.strip() != "":
            tokens.append(extract)

    if len(tokens) % divide != 0:
        append_line_error(bill, parsed, line_index, f"invalid number of tokens for {detail} can't divide by {divide} \n\
                               got: {len(tokens)} \n\
                            tokens: {tokens}")
        
# SECTION 10000

def parse_100000(bill, line_index, parsed):
    tokens = maximum_mobile_tokens(line_index, parsed, bill, 1)

parse_100001 = partial(generic_predicate, field_name="doctype")

def parse_100002(bill, line_index, parsed):
    tokens = maximum_mobile_tokens(line_index, parsed, bill, 1)

parse_100003 =partial(generic_predicate, field_name="100003_clienteCategoria")

def parse_100004(bill, line_index, parsed):
    tokens = maximum_mobile_tokens(line_index, parsed, bill, 1)

def parse_100005(bill, line_index, parsed):
    tokens = maximum_mobile_tokens(line_index, parsed, bill, 1)

def parse_100006(bill, line_index, parsed):
    tokens = maximum_mobile_tokens(line_index, parsed, bill, 1)

def parse_100007(bill, line_index, parsed):
    tokens = maximum_mobile_tokens(line_index, parsed, bill, 1)

def parse_100008(bill, line_index, parsed):
    tokens = maximum_mobile_tokens(line_index, parsed, bill, 1)

def parse_100009(bill, line_index, parsed):
    tokens = maximum_mobile_tokens(line_index, parsed, bill, 1)

def parse_100010(bill, line_index, parsed):
    tokens = maximum_mobile_tokens(line_index, parsed, bill, 1)

def parse_100011(bill, line_index, parsed):
    tokens = maximum_mobile_tokens(line_index, parsed, bill, 1)

def parse_100012(bill, line_index, parsed):
    tokens = maximum_mobile_tokens(line_index, parsed, bill, 1)

def parse_100013(bill, line_index, parsed):
    tokens = maximum_mobile_tokens(line_index, parsed, bill, 1)

def parse_100014(bill, line_index, parsed):
    tokens = maximum_mobile_tokens(line_index, parsed, bill, 1)

def parse_100015(bill, line_index, parsed):
    tokens = maximum_mobile_tokens(line_index, parsed, bill, 1)

def parse_100016(bill, line_index, parsed):
    tokens = maximum_mobile_tokens(line_index, parsed, bill, 1)


parse_100100 = partial(generic_predicate, field_name= "100101_section")
parse_100101 = partial(generic_predicate, field_name="nombre_emisor")

def parse_100102(bill, line_index, parsed):
    tokens = maximum_mobile_tokens(line_index, parsed, bill, 1)

parse_100103 = partial(generic_predicate, field_name="nombre_comercial")

parse_100104 = partial(generic_predicate, field_name="dir_emisor")

def parse_100105(bill, line_index, parsed):
    tokens = maximum_mobile_tokens(line_index, parsed, bill, 1)

def parse_100106(bill, line_index, parsed):
    tokens = maximum_mobile_tokens(line_index, parsed, bill, 1)

def parse_100107(bill, line_index, parsed):
    tokens = maximum_mobile_tokens(line_index, parsed, bill, 1)

def parse_100200(bill, line_index, parsed):
    tokens = maximum_mobile_tokens(line_index, parsed, bill, 1)

parse_100201 = partial(generic_predicate, field_name="cliente_nombre")

def parse_100202(bill, line_index, parsed):
    tokens = maximum_mobile_tokens(line_index, parsed, bill, 1)

def parse_100203(bill, line_index, parsed):
    tokens = maximum_mobile_tokens(line_index, parsed, bill, 1)

parse_100204 = partial(generic_predicate, field_name="cliente_dir_l1")
parse_100205 = partial(generic_predicate, field_name="cliente_dir_l2")
parse_100206 = partial(generic_predicate, field_name="cliente_dir_l3")

def parse_100207(bill, line_index, parsed):
    tokens = maximum_mobile_tokens(line_index, parsed, bill, 1)

parse_100208 = partial(generic_predicate, field_name="100208_clienteEmail")

def parse_100209(bill, line_index, parsed):
    tokens = maximum_mobile_tokens(line_index, parsed, bill, 1)

def parse_100300(bill, line_index, parsed):
    bill._100314 = False
    tokens = maximum_mobile_tokens(line_index, parsed, bill, 1)

def parse_100301(bill, line_index, parsed):
    tokens = maximum_mobile_tokens(line_index, parsed, bill, 1)

def parse_100302(bill, line_index, parsed):
    tokens = maximum_mobile_tokens(line_index, parsed, bill, 1)

def parse_100303(bill, line_index, parsed):
    tokens = maximum_mobile_tokens(line_index, parsed, bill, 1)

def parse_100304(bill, line_index, parsed):
    tokens = maximum_mobile_tokens(line_index, parsed, bill, 1)

def parse_100305(bill, line_index, parsed):
    tokens = maximum_mobile_tokens(line_index, parsed, bill, 1)

def parse_100306(bill, line_index, parsed):
    tokens = maximum_mobile_tokens(line_index, parsed, bill, 1)

def parse_100307(bill, line_index, parsed):
    tokens = maximum_mobile_tokens(line_index, parsed, bill, 1)

def parse_100308(bill, line_index, parsed):
    tokens = maximum_mobile_tokens(line_index, parsed, bill, 1)

def parse_100309(bill, line_index, parsed):
    tokens = maximum_mobile_tokens(line_index, parsed, bill, 1)

def parse_100310(bill, line_index, parsed):
    tokens = maximum_mobile_tokens(line_index, parsed, bill, 1)

def parse_100311(bill, line_index, parsed):
    tokens = maximum_mobile_tokens(line_index, parsed, bill, 1)

def parse_100312(bill, line_index, parsed):
    tokens = maximum_mobile_tokens(line_index, parsed, bill, 1)

parse_100313 = partial(generic_predicate, field_name="total_letras")

def parse_100314(bill, line_index, parsed):
    bill._100314 = True
    line = parsed.predicate[28:].lower().strip()
    if line == "s":
        bill.is_exempt = True
    else:
        bill.is_exempt = False

def parse_100400(bill, line_index, parsed):
    tokens = maximum_mobile_tokens(line_index, parsed, bill, 1)

def parse_100401(bill, line_index, parsed):
    tokens = maximum_mobile_tokens(line_index, parsed, bill, 1)

def parse_100402(bill, line_index, parsed):
    tokens = maximum_mobile_tokens(line_index, parsed, bill, 1)

def parse_100403(bill, line_index, parsed):
    tokens = maximum_mobile_tokens(line_index, parsed, bill, 1)

def parse_100404(bill, line_index, parsed):
    tokens = maximum_mobile_tokens(line_index, parsed, bill, 1)

parse_100405 = partial(generic_predicate, field_name="100405_courierRuta")

def parse_100406(bill, line_index, parsed):
    tokens = maximum_mobile_tokens(line_index, parsed, bill, 1)

def parse_100407(bill, line_index, parsed):
    tokens = maximum_mobile_tokens(line_index, parsed, bill, 1)

parse_100408 = partial(generic_predicate, field_name="resolucion")

def parse_100409(bill, line_index, parsed):
    tokens = maximum_mobile_tokens(line_index, parsed, bill, 1)

parse_100410 = partial(generic_predicate, field_name="observaciones")

def parse_100430(bill, line_index, parsed):
    tokens = maximum_mobile_tokens(line_index, parsed, bill, 1)

def parse_100500(bill, line_index, parsed):
    tokens = maximum_mobile_tokens(line_index, parsed, bill, 2)

parse_100501 = partial(generic_predicate, field_name="txt1")
parse_100502 = partial(generic_predicate, field_name="txt2")
parse_100503 = partial(generic_predicate, field_name="txt3")

def parse_100600(bill, line_index, parsed):
    tokens = maximum_mobile_tokens(line_index, parsed, bill, 1) 

def parse_100602(bill, line_index, parsed):
    maximum_tokens = 3
    if bill._100314:
        if bill.is_exempt:
            maximum_tokens = 4
    tokens = parsed.predicate[71:].strip().split("Q")
    tokens = list(filter(None, tokens))
    if len(tokens) != maximum_tokens:
        append_line_error(bill, parsed, line_index, f"invalid number of tokens\n\
                expecting: {maximum_tokens}\n\
                      got: {len(tokens)}")

def parse_100603(bill, line_index, parsed):
    maximum_tokens = 3
    if bill._100314:
        if bill.is_exempt:
            maximum_tokens = 4
    tokens = parsed.predicate[71:].strip().split("Q")
    tokens = list(filter(None, tokens))
    if len(tokens) != maximum_tokens:
        append_line_error(bill, parsed, line_index, f"invalid number of tokens\n\
                expecting: {maximum_tokens}\n\
                      got: {len(tokens)}")

def parse_100604(bill, line_index, parsed):
    tokens = maximum_mobile_tokens(line_index, parsed, bill, 2)

def parse_200000(bill, line_index, parsed):
    tokens = maximum_mobile_tokens(line_index, parsed, bill, 1)

def parse_200003(bill, line_index, parsed):
    range_list = [(24,99)]
    tokens = remove_string_segments(parsed.predicate, range_list)
    if len(tokens)!= 3:
        append_line_error(bill, parsed, line_index, "unexpected tokens length")

parse_200004 = partial(generic_predicate, field_name="20004_total")

def parse_200100(bill, line_index, parsed):
    bill._200101 = False
    
def parse_200101(bill, line_index, parsed):
    bill._200101 = True
    bill._200102 = False
    
def parse_200102(bill, line_index, parsed):
    if bill._200101 == False:
        append_line_error(bill, parsed, line_index, "error in hierarchy")
    bill._200102 = True
    bill._200103 = False

def parse_200103(bill, line_index, parsed):
    if bill._200102 == False:
        append_line_error(bill, parsed, line_index, "error in hierarchy")
    bill._200103 = True
    bill._200104 = False

    tokens = maximum_mobile_tokens(line_index, parsed, bill, 2)

def parse_200104(bill, line_index, parsed):
    if bill._200103 == False:
        append_line_error(bill, parsed, line_index, "error in hierarchy")

    tokens = parsed.predicate[80:].split()
    if len(tokens) > 2:
        append_line_error(bill, parsed, line_index, "number of tokens exceded")

def parse_200105(bill, line_index, parsed):
    tokens = maximum_mobile_tokens(line_index, parsed, bill, 2)

def parse_200200(bill, line_index, parsed):
    bill._200201 = False

def parse_200201(bill, line_index, parsed):
    bill._200201 = True
    bill._200202 = False

def parse_200202(bill, line_index, parsed):
    if bill._200201 == False:
        append_line_error(bill, parsed, line_index, "error in hierarchy")
    bill._200202 = True
    bill._200203 = False

def parse_200203(bill, line_index, parsed):
    if bill._200202 == False:
        append_line_error(bill, parsed, line_index, "error in hierarchy")
    bill._200203 = True
    bill._200204 = False

def parse_200204(bill, line_index, parsed):
    if bill._200203 == False:
        append_line_error(bill, parsed, line_index, "error in hierarchy")
    bill._200204 = True
    bill._200205 = False

def parse_200205(bill, line_index, parsed):
    if bill._200204 == False:
        append_line_error(bill, parsed, line_index, "error in hierarchy")
    bill._200205 = True
    bill._200206 = False

    tokens = maximum_mobile_tokens(line_index, parsed, bill, 2)
    
def parse_200206(bill, line_index, parsed):
    if bill._200205 == False:
        append_line_error(bill, parsed, line_index, "error in hierarchy")
    bill._200206 = True
    bill._200207 = False

    tokens = parsed.predicate[86:].split()
    tokens_length = len(tokens)
    if tokens_length > 0:
        if tokens_length > 2:
            append_line_error(bill, parsed, line_index, "exceeded tokens")
            
def parse_200207(bill, line_index, parsed):
    if bill._200206 == False:
        append_line_error(bill, parsed, line_index, "error in hierarchy")
    # bill._200207 = True
    # bill._200208 = False

def parse_200300(bill, line_index,parsed):
    bill._200301 = False
    
    
def parse_200301(bill, line_index, parsed):
    bill._200301 = True
    bill._200305 = False
    bill._200306 = False

def parse_200302(bill, line_index, parsed):
    if bill._200301 == False:
        append_line_error(bill, parsed, line_index, "error in hierarchy")

def parse_200303(bill, line_index, parsed):
    if bill._200301 == False:
        append_line_error(bill, parsed, line_index, "error in hierarchy")
    

def parse_200304(bill, line_index, parsed):
    if bill._200301 == False:
        append_line_error(bill, parsed, line_index, "error in hierarchy")

def parse_200305(bill, line_index, parsed):
    if bill._200301 == False:
        append_line_error(bill, parsed, line_index, "error in hierarchy")
    bill._200305 = True
    
def parse_200306(bill, line_index, parsed):
    if bill._200305 == False:
        append_line_error(bill, parsed, line_index, "error in hierarchy")
    bill._200306 = True
    bill._200307 = False
    
def parse_200307(bill, line_index, parsed):
    if bill._200306 == False:
        append_line_error(bill, parsed, line_index, "error in hierarchy")
    
def parse_200308(bill, line_index, parsed):
    if bill._200301 == False:
        append_line_error(bill, parsed, line_index, "error in hierarchy")
    
def parse_200400(bill,line_index, parsed):
    pass
            
parse_200500 = partial(generic_predicate, field_name="200500_seccion")

def parse_200503(bill, line_index, parsed):
    # range_list = [(20, 85)]
    no_middle_dash = parsed.predicate.replace("-", " ")
    line = no_middle_dash
    
    first_tokens = line[:89].split()
    values = line[89:].split('Q')
        
    list_elements = []
    expected_tokens = 7
    # rango = 2
    # pattern = r'\d{8}'
    # if not re.search(pattern, first_tokens[1]):
    #     rango -= 1
    #     expected_tokens -= 1
    
    for i in range(2):
        list_elements.append(first_tokens.pop(0))
    
    product = " ".join(first_tokens)
    if product != "":
        list_elements.append(product)
    
    for token in values:
        list_elements.append(token.strip())
        
    if len(list_elements) != expected_tokens:
        append_line_error(bill, parsed, line_index, 
                            f"missing tokens, expecting {expected_tokens}, given {len(list_elements)}\n \
                           tokens: {list_elements}")

parse_200600 = partial(generic_predicate, field_name="200600_seccion")

def parse_200603(bill, line_index, parsed):
    tokens = parsed.predicate[80:].split() 
    if len(tokens) > 8:
        append_line_error(bill, parsed, line_index, "more than 8 tokens")

def parse_300000(bill, line_index, parsed):
    bill._300100 = False
    bill._300101 = False
    
def parse_300100(bill, line_index, parsed):
    bill._300100 = True
def parse_300101(bill, line_index, parsed):
    if bill._300100 == False:
        append_line_error(bill, parsed, line_index, "error in hierarchy")
    bill._300101 = True
    bill._300102 = False

def parse_300102(bill, line_index, parsed):
    if bill._300101 == False:
        append_line_error(bill, parsed, line_index, "error in hierarchy")
    bill._300102 = True
    bill._300103 = False
    
    bill._300102_predicate = parsed.predicate

def parse_300103(bill, line_index, parsed):
    if bill._300102 == False:
        append_line_error(bill, parsed, line_index, "error in hierarchy")
    bill._300103 = True
    bill._300104 = False


def parse_300104(bill, line_index, parsed):
    if bill._300103 == False:
        append_line_error(bill, parsed, line_index, "error in hierarchy")

    detail = bill._300102_predicate.strip()
    if detail == "2CLARO ENTRETENIMIENTO":
        parse_by_consumption_detail(line_index, parsed, bill, [(20,66),(103,148)], 5)
        
    elif detail == "2DETALLE DE LLAMADAS SALIENTES LOCAL":
        parse_by_consumption_detail(line_index, parsed, bill, [(40,65),(127,153)], 6)
    
    elif detail == "2DETALLE DE MENSAJES DE TEXTO LOCAL":

        pipe_splitted = parsed.predicate.strip().split("|")
        tokens_length = 0

        for column in pipe_splitted:
            if column != "":
                tokens_length += 6
                tokens = column.split()
                match = re.search(r'\d+', tokens[2])
                if not match:
                    tokens_length -= 1

        range_list = [(20,41),(42,65),(104,124),(125,148)]            

        pipe_removed = parsed.predicate.replace("|", " ")
        detail = bill._300102_predicate.strip()
        tokens = remove_string_segments(pipe_removed, range_list)
        extracted = extract_sections(pipe_removed, range_list)
        for extract in extracted:
            if extract.strip() != "":
                tokens.append(extract)

        if len(tokens) != tokens_length:
            append_line_error(bill, parsed, line_index, f"invalid number of tokens for {detail}\n\
                                expected: {tokens_length}\n\
                                got: {len(tokens)} \n\
                                tokens: {tokens}")

    elif detail == "2DETALLE DE LLAMADAS EN ROAMING SIN FRONTERAS":
        incoming = parsed.predicate[40:136].strip().split()
        if incoming[0] == "Llamada":
            divide = 5
        else:
            divide = 6
        parse_by_consumption_detail(line_index, parsed, bill, [(20,39),(40,136)], divide)
    
    elif detail == "2DETALLE DE MENSAJES DE TEXTO EN ROAMING SIN FRONTERAS":
        parse_by_consumption_detail(line_index, parsed, bill, [(40,65),(123,149)], 6)
    
    elif detail == "2DETALLE DE LLAMADAS DE LARGA DISTANCIA INTERNACIONAL SIN FRONTERAS":
        parse_by_consumption_detail(line_index, parsed, bill, [(40,63),(123,146)], 6)
    
    elif detail == "2DETALLE DE LLAMADAS DE LARGA DISTANCIA INTERNACIONAL":
        parse_by_consumption_detail(line_index, parsed, bill, [(40,64),(123,147)], 6)
    
    elif detail == "2DETALLE DE MENSAJES DE TEXTO EN ROAMING":
        parse_by_consumption_detail(line_index, parsed, bill, [(40,65),(123,148)], 6)
        
    elif detail == "2PRODUCTIVIDAD":
        parse_by_consumption_detail(line_index, parsed, bill, [(20,65),(103,148)], 5)
    
    elif detail == "2DETALLE DE LLAMADAS POR COBRAR":
        parse_by_consumption_detail(line_index, parsed, bill, [(40,65),(127,153)], 6)
    
    elif detail == "2DETALLE DE RECARGAS PROGRAMADAS POR EVENTO":
        parse_by_consumption_detail(line_index, parsed, bill, [(39,65),(122,148)], 6)
    
    elif detail == "2SERVICIOS VARIOS":
        parse_by_consumption_detail(line_index, parsed, bill, [(20,65),(103,148)], 5)
    
    elif detail == "2DETALLE DE LLAMADAS ILIMITADAS":
        parse_by_consumption_detail(line_index, parsed, bill, [(40,65),(127,153)], 6)

    elif detail == "2DETALLE DE LLAMADAS ENTRANTES":
        parse_by_consumption_detail(line_index, parsed, bill, [(38,63),(127,146)], 5)

    elif detail == "2DETALLE DE MENSAJES DE TEXTO EN ROAMING ENTRANTE":
        parse_by_consumption_detail(line_index, parsed, bill, [(38,63),(121,146)], 6)

    elif detail == "2DETALLE DE VIDEO LLAMADAS":
        parse_by_consumption_detail(line_index, parsed, bill, [(38,66),(125,154)], 6)

    elif detail == "2DETALLE LLAMADAS POR COBRAR":
        line = parsed.predicate.replace("|", " ")
        tokens = line.split()
        if len(tokens) % 5 != 0:
            append_line_error(bill, parsed, line_index, "invalid number of tokens, can't divide by 5")
        
    elif detail.startswith("2DETALLE DE SUSCRIPCI"):
        parse_by_consumption_detail(line_index, parsed, bill, [(20,65),(103,148)], 5)
    
    elif detail.startswith("2DETALLE DE NAVEGACI"):
        parse_by_consumption_detail(line_index, parsed, bill, [(20,50),(103,133)], 5)
        
    elif detail == "2DETALLE DE LLAMADAS EN ROAMING":
        incoming = parsed.predicate[40:138].strip().split()
        if incoming[0] == "Llamada":
            divide = 5
        else:
            divide = 6
        parse_by_consumption_detail(line_index, parsed, bill, [(40,138)], divide)
        
    # else:
    #     append_line_error(bill, parsed, line_index, f"not implemented consumption detail {detail}")
        

def parse_300105(bill, line_index, parsed):
    if bill._300102 == False:
        append_line_error(bill, parsed, line_index, "error in hierarchy")
    
    tokens = parsed.predicate[25:].split()
    if len(tokens) > 2:
        append_line_error(bill, parsed, line_index, "more than 2 tokens")
    

def parse_400000(bill, line_index, parsed):
    bill._400001 = False

def parse_400001(bill, line_index, parsed):
    bill._400001 = True

    tokens = parsed.predicate.split()
    if len(tokens) > 2:
        append_line_error(bill, parsed, line_index, "more than 2 tokens")       

def parse_400002(bill, line_index, parsed):
    if bill._400001 == False:
        append_line_error(bill, parsed, line_index, "error in hierarchy")
    bill._400002 = True
    bill._400003 = False

def parse_400003(bill, line_index, parsed):
    if bill._400002 == False:
        append_line_error(bill, parsed, line_index, "error in hierarchy")
    bill._400003 = True
    bill._400004 = False

    tokens = parsed.predicate.split()
    if len(tokens) != 4:
        append_line_error(bill, parsed, line_index, "not 4 tokens") 

def parse_400004(bill, line_index, parsed):
    if bill._400003 == False:
        append_line_error(bill, parsed, line_index, "error in hierarchy")
    bill._400004 = True
    bill._400005 = False

    range_list = [(24,54),(82,150)]
    tokens = remove_string_segments(parsed.predicate, range_list)
    if len(tokens) != 3:
        append_line_error(bill, parsed, line_index, "not 3 tokens") 

def parse_400005(bill, line_index, parsed):
    if bill._400004 == False:
        append_line_error(bill, parsed, line_index, "error in hierarchy")
    bill._400005 = True
    bill._400006 = False

    range_list = [(1,20)]
    tokens = remove_string_segments(parsed.predicate, range_list)
    if len(tokens) != 2:
        append_line_error(bill, parsed, line_index, "not 2 tokens")     
    
def parse_400006(bill, line_index, parsed):
    bill._400006 = True
    
def parse_900000(bill, line_index, parsed):
    pass
