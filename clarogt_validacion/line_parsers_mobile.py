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

def parse_by_consumption_detail(line_index, parsed, bill, range_list, divide):
    pipe_removed = parsed.predicate.replace("|", " ")
    detail = bill._300102_predicate.strip()
    tokens = remove_string_segments(pipe_removed, range_list)
    if len(tokens) % divide != 0:
        append_line_error(bill, parsed, line_index, f"invalid number of tokens for {detail} can't divide by {divide} \n                         tokens: {tokens}")

def get_expected_position(bill, line_index, parsed, expected):
    expected = expected
    sequence = parsed.predicate[0]
    if sequence != expected:
        append_line_error(bill, parsed, line_index,
                          f"error at parsing predicate sequence, \n\
                          expecting: {expected}, got: {sequence}")
        
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
    tokens = maximum_mobile_tokens(line_index, parsed, bill, 6)
    # if len(tokens) != 6:
    #     append_line_error(bill, parsed, line_index, "not 6 tokens")

def parse_100603(bill, line_index, parsed):
    tokens = maximum_mobile_tokens(line_index, parsed, bill, 6)

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
    bill._200101_exists = False
    bill._200102_exists = False
    bill._200103_exists = False
    bill._200104_exists = False
    bill._200105_exists = False
    bill._200100_hasdetails = False

def parse_200101(bill, line_index, parsed):
    bill._200101_exists = True
    bill._200100_hasdetails = True
    
def parse_200102(bill, line_index, parsed):
    bill._200102_exists = True
    bill._200100_hasdetails = True

def parse_200103(bill, line_index, parsed):
    tokens = maximum_mobile_tokens(line_index, parsed, bill, 2)

    bill._200103_exists = True
    bill._200100_hasdetails = True

def parse_200104(bill, line_index, parsed):
    tokens = parsed.predicate[80:].split()
    if len(tokens) > 2:
        append_line_error(bill, parsed, line_index, "number of tokens exceded")
    
    bill._200104_exists = True
    bill._200100_hasdetails = True

def parse_200105(bill, line_index, parsed):
    tokens = maximum_mobile_tokens(line_index, parsed, bill, 2)
    
    bill._200105_exists = True
    bill._200100_hasdetails = True

def parse_200200(bill, line_index, parsed):
    if bill._200100_hasdetails:
        details = {
            "200101": bill._200101_exists,
            "200102": bill._200102_exists,
            "200103": bill._200103_exists,
            "200104": bill._200104_exists,
            "200105": bill._200105_exists,
        }
        details_list = [key for key, value in details.items() if not value]
        if details_list:
            append_line_error(bill, parsed, line_index, f"missing details sections {details_list}")
    
    bill._200201_exists = False
    bill._200202_exists = False
    bill._200203_exists = False
    bill._200204_exists = False
    bill._200205_exists = False
    bill._200206_exists = False
    bill._200207_exists = False
    bill._200200_hasdetails = False

def parse_200201(bill, line_index, parsed):
    bill._200201_exists = True
    bill._200200_hasdetails = True

def parse_200202(bill, line_index, parsed):
    bill._200202_exists = True
    bill._200200_hasdetails = True

def parse_200203(bill, line_index, parsed):
    bill._200203_exists = True
    bill._200200_hasdetails = True

def parse_200204(bill, line_index, parsed):
    bill._200204_exists = True
    bill._200200_hasdetails = True

def parse_200205(bill, line_index, parsed):
    tokens = maximum_mobile_tokens(line_index, parsed, bill, 2)
    
    bill._200205_exists = True
    bill._200200_hasdetails = True

def parse_200206(bill, line_index, parsed):
    tokens = parsed.predicate[86:].split()
    tokens_length = len(tokens)
    if tokens_length > 0:
        if tokens_length > 2:
            append_line_error(bill, parsed, line_index, "exceeded tokens")
            
    bill._200206_exists = True
    bill._200200_hasdetails = True

def parse_200207(bill, line_index, parsed):
    bill._200207_exists = True
    bill._200200_hasdetails = True

def parse_200300(bill, line_index,parsed):
    if bill._200200_hasdetails:
        details = {
            "200201": bill._200201_exists,
            "200202": bill._200202_exists,
            "200203": bill._200203_exists,
            "200204": bill._200204_exists,
            "200205": bill._200205_exists,
            "200206": bill._200206_exists,
            "200207": bill._200207_exists,
        }
        details_list = [key for key, value in details.items() if not value]
        if details_list:
            append_line_error(bill, parsed, line_index, f"missing details sections {details_list}")
    
    bill._200301_exists = False
    bill._200302_exists = False
    bill._200303_exists = False
    bill._200304_exists = False
    bill._200305_exists = False
    bill._200306_exists = False
    bill._200307_exists = False
    bill._200308_exists = False
    bill._200300_hasdetails = False
    
def parse_200301(bill, line_index, parsed):
    bill._200301_exists = True
    bill._200300_hasdetails = True
    
    get_expected_position(bill, line_index, parsed, "1")

def parse_200302(bill, line_index, parsed):
    bill._200302_exists = True
    bill._200300_hasdetails = True
    
    get_expected_position(bill, line_index, parsed, "2")

def parse_200303(bill, line_index, parsed):
    bill._200303_exists = True
    bill._200300_hasdetails = True
    
    get_expected_position(bill, line_index, parsed, "3")

def parse_200304(bill, line_index, parsed):
    bill._200304_exists = True
    bill._200300_hasdetails = True
    
    get_expected_position(bill, line_index, parsed, "4")

def parse_200305(bill, line_index, parsed):
    tokens = maximum_mobile_tokens(line_index, parsed, bill, 2)
    bill._200305_exists = True
    bill._200300_hasdetails = True
    
    get_expected_position(bill, line_index, parsed, "5")

def parse_200306(bill, line_index, parsed):
    tokens = parsed.predicate[80:].split()
    bill._200306_exists = True
    bill._200300_hasdetails = True
    
    get_expected_position(bill, line_index, parsed, "6")
    
def parse_200307(bill, line_index, parsed):
    bill._200307_exists = True
    bill._200300_hasdetails = True
    
    get_expected_position(bill, line_index, parsed, "7")
    
def parse_200308(bill, line_index, parsed):
    bill._200308_exists = True
    bill._200300_hasdetails = True
    
    get_expected_position(bill, line_index, parsed, "8")
    
def parse_200400(bill,line_index, parsed):
    if bill._200300_hasdetails:
        details = {
            "200301": bill._200301_exists,
            "200302": bill._200302_exists,
            "200303": bill._200303_exists,
            "200304": bill._200304_exists,
            "200305": bill._200305_exists,
            "200306": bill._200306_exists,
            "200307": bill._200307_exists,
            "200308": bill._200308_exists,
        }
        details_list = [key for key, value in details.items() if not value]
        if details_list:
            append_line_error(bill, parsed, line_index, f"missing details sections {details_list}")
            
            
parse_200500 = partial(generic_predicate, field_name="200500_seccion")

def parse_200503(bill, line_index, parsed):
    # range_list = [(20, 85)]
    nohyphen = parsed.predicate.replace("-", " ")
    line = nohyphen[1:]
    values = line[89:].split('Q')
    
    expected_tokens = 10
    if values[0].strip() == "":
        expected_tokens -= 1
        
    for char in line:
        if char.isalpha():
            description_index = line.index(char)
            break
        
    description_range_list = [(description_index, 85)]
    description = line[description_index:85]
    
    first_tokens = line[:description_index].split()
    if len(first_tokens) == 1:
        expected_tokens -= 1
    
    tokens = remove_string_segments(line, description_range_list)
    if description.strip() != "":
        tokens.append(description.strip())
    # else:
    #     append_line_error(bill, parsed, line_index, "missing description token")
        
    if len(tokens) != expected_tokens:
        append_line_error(bill, parsed, line_index, 
                            f"missing tokens, expecting {expected_tokens}, given {len(tokens)}\n \
                           tokens: {tokens}")
        

parse_200600 = partial(generic_predicate, field_name="200600_seccion")

def parse_200603(bill, line_index, parsed):
    tokens = parsed.predicate[80:].split() 
    if len(tokens) > 8:
        append_line_error(bill, parsed, line_index, "more than 8 tokens")

def parse_300000(bill, line_index, parsed):
    bill._300100_exists = False
    bill._300101_exists = False
    bill._300102_exists = False
    bill._300103_exists = False
    bill._300104_exists = False
    bill._300105_exists = False
    
    bill._300000_hasdetails = False

def parse_300100(bill, line_index, parsed):
    tokens = maximum_mobile_tokens(line_index, parsed, bill, 1)
    
    bill._300100_exists = True
    bill._300000_hasdetails = True

def parse_300101(bill, line_index, parsed):
    bill._300101_exists = True
    bill._300000_hasdetails = True

def parse_300102(bill, line_index, parsed):
    bill._300102_predicate = parsed.predicate.strip()
    
    bill._300102_exists = True
    bill._300000_hasdetails = True
    
def parse_300103(bill, line_index, parsed):
    # tokens = parsed.predicate[1:].split()
    bill._300103_exists = True
    bill._300000_hasdetails = True

def parse_300104(bill, line_index, parsed):
    detail = bill._300102_predicate.strip()
    if detail == "2CLARO ENTRETENIMIENTO":
        parse_by_consumption_detail(line_index, parsed, bill, [(20,66),(103,148)], 2)
        
    elif detail == "2DETALLE DE LLAMADAS SALIENTES LOCAL":
        parse_by_consumption_detail(line_index, parsed, bill, [(40,65),(127,153)], 5)
    
    elif detail == "2DETALLE DE MENSAJES DE TEXTO LOCAL":
        parse_by_consumption_detail(line_index, parsed, bill, [(40,65),(123,148)], 5)
        
    elif detail == "2DETALLE DE LLAMADAS EN ROAMING SIN FRONTERAS":
        parse_by_consumption_detail(line_index, parsed, bill, [(20,138)], 2)
    
    elif detail == "2DETALLE DE MENSAJES DE TEXTO EN ROAMING SIN FRONTERAS":
        parse_by_consumption_detail(line_index, parsed, bill, [(40,65),(123,149)], 5)
    
    elif detail == "2DETALLE DE LLAMADAS DE LARGA DISTANCIA INTERNACIONAL SIN FRONTERAS":
        parse_by_consumption_detail(line_index, parsed, bill, [(40,64),(123,146)], 5)
    
    elif detail == "2DETALLE DE LLAMADAS DE LARGA DISTANCIA INTERNACIONAL":
        parse_by_consumption_detail(line_index, parsed, bill, [(40,64),(123,147)], 5)
    
    elif detail == "2DETALLE DE MENSAJES DE TEXTO EN ROAMING":
        parse_by_consumption_detail(line_index, parsed, bill, [(40,65),(123,148)], 5)
        
    elif detail == "2PRODUCTIVIDAD":
        parse_by_consumption_detail(line_index, parsed, bill, [(20,65),(103,148)], 4)
    
    elif detail == "2DETALLE DE LLAMADAS POR COBRAR":
        parse_by_consumption_detail(line_index, parsed, bill, [(40,65),(127,153)], 5)
    
    elif detail == "2DETALLE DE RECARGAS PROGRAMADAS POR EVENTO":
        parse_by_consumption_detail(line_index, parsed, bill, [(39,65),(122,148)], 5)
    
    elif detail == "2SERVICIOS VARIOS":
        parse_by_consumption_detail(line_index, parsed, bill, [(20,65),(103,148)], 4)
    
    elif detail == "2DETALLE DE LLAMADAS ILIMITADAS":
        parse_by_consumption_detail(line_index, parsed, bill, [(40,65),(127,153)], 5)
        
    elif detail.startswith("2DETALLE DE SUSCRIPCI"):
        parse_by_consumption_detail(line_index, parsed, bill, [(20,65),(103,148)], 4)
    
    elif detail.startswith("2DETALLE DE NAVEGACI"):
        parse_by_consumption_detail(line_index, parsed, bill, [(20,50),(103,133)], 4)
        
    elif detail == "2DETALLE DE LLAMADAS EN ROAMING":
        incoming = parsed.predicate[40:138].strip().split()
        if incoming[0] == "Llamada":
            divide = 4
        else:
            divide = 5
        parse_by_consumption_detail(line_index, parsed, bill, [(40,138)], divide)
        
    # else:
    #     append_line_error(bill, parsed, line_index, f"not implemented consumption detail {detail}")
        
    bill._300104_exists = True
    bill._300000_hasdetails = True

def parse_300105(bill, line_index, parsed):
    tokens = parsed.predicate[25:].split()
    if len(tokens) > 2:
        append_line_error(bill, parsed, line_index, "more than 2 tokens")
    
    bill._300105_exists = True
    bill._300000_hasdetails = True

def parse_400000(bill, line_index, parsed):
    if bill._300000_hasdetails:
        details = {
            "300100": bill._300100_exists,
            "300101": bill._300101_exists,
            "300102": bill._300102_exists,
            "300103": bill._300103_exists,
            "300104": bill._300104_exists,
            "300105": bill._300105_exists,
        }
        details_list = [key for key, value in details.items() if not value]
        if details_list:
            append_line_error(bill, parsed, line_index, f"missing details sections {details_list}")
    
    bill._400001_exists = False
    bill._400002_exists = False
    bill._400003_exists = False
    bill._400004_exists = False
    bill._400005_exists = False
    bill._400006_exists = False
    bill._400000_hasdetails = False

def parse_400001(bill, line_index, parsed):
    tokens = parsed.predicate.split()
    if len(tokens) > 2:
        append_line_error(bill, parsed, line_index, "more than 2 tokens")
        
    bill._400001_exists = True
    bill._400000_hasdetails = True

def parse_400002(bill, line_index, parsed):
    bill._400002_exists = True
    bill._400000_hasdetails = True

def parse_400003(bill, line_index, parsed):
    tokens = parsed.predicate.split()
    if len(tokens) != 4:
        append_line_error(bill, parsed, line_index, "not 4 tokens")
    
    bill._400003_exists = True
    bill._400000_hasdetails = True

def parse_400004(bill, line_index, parsed):
    range_list = [(24,54),(82,150)]
    tokens = remove_string_segments(parsed.predicate, range_list)
    if len(tokens) != 3:
        append_line_error(bill, parsed, line_index, "not 3 tokens")
    
    bill._400004_exists = True
    bill._400000_hasdetails = True

def parse_400005(bill, line_index, parsed):
    range_list = [(1,20)]
    tokens = remove_string_segments(parsed.predicate, range_list)
    if len(tokens) != 2:
        append_line_error(bill, parsed, line_index, "not 2 tokens")
        
    bill._400005_exists = True
    bill._400000_hasdetails = True
    
def parse_400006(bill, line_index, parsed):
    bill._400006_exists = True
    bill._400000_hasdetails = True
    
def parse_900000(bill, line_index, parsed):
    if bill._400000_hasdetails:
        details = {
            "400001": bill._400001_exists,
            "400002": bill._400002_exists,
            "400003": bill._400003_exists,
            "400004": bill._400004_exists,
            "400005": bill._400005_exists,
            "400006": bill._400006_exists,
        }
        details_list = [key for key, value in details.items() if not value]
        if details_list:
            append_line_error(bill, parsed, line_index, f"missing details sections {details_list}")
