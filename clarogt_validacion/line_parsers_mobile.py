from datetime import datetime
from functools import partial
from collections import namedtuple
from models import LineError
from ioutils import file_stream_reader
from helpers import *
import re

# SECTION 10000
def strip_mobile(string):
    return string[28:]

def maximum_mobile_tokens(line_index, parsed, bill, split_len):
    string = parsed.predicate[28:]
    tokens = string.split()
    if len(tokens) > split_len:
        append_line_error(bill, parsed, line_index, "exceeded tokens")
        return None
    return  tokens

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
    tokens = maximum_mobile_tokens(line_index, parsed, bill, 1)

parse_200101 = partial(generic_predicate, field_name="200101")
parse_200102 = partial(generic_predicate, field_name="200102")

def parse_200103(bill, line_index, parsed):
    tokens = maximum_mobile_tokens(line_index, parsed, bill, 2)

def parse_200104(bill, line_index, parsed):
    tokens = parsed.predicate[80:].split()
    if len(tokens) > 2:
        append_line_error(bill, parsed, line_index, "number of tokens exceded")

def parse_200105(bill, line_index, parsed):
    tokens = maximum_mobile_tokens(line_index, parsed, bill, 2)

def parse_200200(bill, line_index, parsed):
    tokens = maximum_mobile_tokens(line_index, parsed, bill, 1)

parse_200201 = partial(generic_predicate, field_name="200201")
parse_200202 = partial(generic_predicate, field_name="200202")
parse_200203 = partial(generic_predicate, field_name="200203")
parse_200204 = partial(generic_predicate, field_name="200204")

def parse_200205(bill, line_index, parsed):
    tokens = maximum_mobile_tokens(line_index, parsed, bill, 2)
    # if len(tokens) != 2:
    #     append_line_error(bill, parsed, line_index, "not 2 tokens")

def parse_200206(bill, line_index, parsed):
    tokens = parsed.predicate[86:].split()
    tokens_length = len(tokens)
    if tokens_length > 0:
        if tokens_length > 2:
            append_line_error(bill, parsed, line_index, "exceeded tokens")
        

parse_200207 = partial(generic_predicate, field_name="200207_totalPAgar")

parse_200300 = partial(generic_predicate, field_name="200300_seccion")
parse_200301 = partial(generic_predicate, field_name="200301_codigo_Cliente")
parse_200302 = partial(generic_predicate, field_name="200302_telefono")
parse_200303 = partial(generic_predicate, field_name="200303_razonSocial")
parse_200304 = partial(generic_predicate, field_name="200304_contrato")

def parse_200305(bill, line_index, parsed):
    tokens = maximum_mobile_tokens(line_index, parsed, bill, 2)

def parse_200306(bill, line_index, parsed):
    tokens = maximum_mobile_tokens(line_index, parsed, bill, 2)

parse_200307 = partial(generic_predicate, field_name="200307_subPlanSeccion")
parse_200308 = partial(generic_predicate, field_name="200308_totalPagar")

parse_200400 = partial(generic_predicate, field_name="200400_seccion")

parse_200500 = partial(generic_predicate, field_name="200500_seccion")

def parse_200503(bill, line_index, parsed):
    tokens = parsed.predicate[80:].split() 
    if len(tokens) != 5:
        append_line_error(bill, parsed, line_index, "not 5 tokens")

parse_200600 = partial(generic_predicate, field_name="200600_seccion")

def parse_200603(bill, line_index, parsed):
    tokens = parsed.predicate[80:].split() 
    if len(tokens) != 5:
        append_line_error(bill, parsed, line_index, "not 5 tokens")

parse_300000 = partial(generic_predicate, field_name="300000_seccion")

def parse_300100(bill, line_index, parsed):
    tokens = maximum_mobile_tokens(line_index, parsed, bill, 1)

parse_300101 = partial(generic_predicate, field_name="300101_detalleConsumos")

parse_300102 = partial(generic_predicate, field_name="300102_detalleConsumos")

def parse_300103(bill, line_index, parsed):
    tokens = maximum_mobile_tokens(line_index, parsed, bill, 12)

def parse_300104(bill, line_index, parsed):
    range_list = [(40,65),(123,149)]
    tokens = remove_string_segments(parsed.predicate, range_list)
    if len(tokens) % 5 != 0:
        print(tokens)
        append_line_error(bill, parsed, line_index, "invalid number of rows, can't divide by 5")

def parse_300105(bill, line_index, parsed):
    tokens = parsed.predicate[25:].split()
    if len(tokens) > 2:
        append_line_error(bill, parsed, line_index, "more than 2 tokens")

parse_400000 = partial(generic_predicate, field_name="400000_seccion")

def parse_400001(bill, line_index, parsed):
    tokens = parsed.predicate.split()
    if len(tokens) > 2:
        append_line_error(bill, parsed, line_index, "more than 2 tokens")

parse_400002 = partial(generic_predicate, field_name="400002_ajusteSeccion")

def parse_400003(bill, line_index, parsed):
    tokens = parsed.predicate.split()
    if len(tokens) != 4:
        append_line_error(bill, parsed, line_index, "not 4 tokens")

def parse_400004(bill, line_index, parsed):
    range_list = [(24,54),(82,-1)]
    tokens = remove_string_segments(parsed.predicate, range_list)
    if len(tokens) != 3:
        append_line_error(bill, parsed, line_index, "not 3 tokens")

def parse_400005(bill, line_index, parsed):
    range_list = [(1,20)]
    tokens = remove_string_segments(parsed.predicate, range_list)
    if len(tokens) != 2:
        append_line_error(bill, parsed, line_index, "not 2 tokens")

parse_400006 = partial(generic_predicate, field_name="400006_NCRE")
