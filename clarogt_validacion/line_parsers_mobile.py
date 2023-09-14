from datetime import datetime
from functools import partial
from collections import namedtuple
from models import LineError
from ioutils import file_stream_reader
from helpers import *
import re

# SECTION 10000

parse_100000 = partial(generic_predicate, field_name= "100000_seccion")   
parse_100001 = partial(generic_predicate, field_name= "100001_docType")
parse_100002 = partial(generic_predicate, field_name= "100002_docNum")
parse_100003 = partial(generic_predicate, field_name= "100003_clientCat")
parse_100004 = partial(generic_predicate, field_name= "100004_clientCycle")
parse_100005 = partial(generic_predicate, field_name= "100005_initPeriod")
parse_100006 = partial(generic_predicate, field_name= "100006_endPeriod")
parse_100007 = partial(generic_predicate, field_name= "100007_acredDate")
parse_100008 = partial(generic_predicate, field_name= "100008_emmitionDate")
parse_100009 = partial(generic_predicate, field_name= "100009_limitPayDate")
parse_100010 = partial(generic_predicate, field_name= "100010_noContracts")
parse_100011 = partial(generic_predicate, field_name= "100011_series")
parse_100012 = partial(generic_predicate, field_name= "100012_preprinted")
parse_100013 = partial(generic_predicate, field_name= "100013_authNumber")
parse_100014 = partial(generic_predicate, field_name= "100014_adminSeries")
parse_100015 = partial(generic_predicate, field_name= "100015_adminNumber")
parse_100016 = partial(generic_predicate, field_name= "100016_trafficDetail")

def parse_100100(bill, line_index, parsed):
    pass

def parse_100101(bill, line_index, parsed):
    pass

def parse_100102(bill, line_index, parsed):
    pass

def parse_100103(bill, line_index, parsed):
    pass

def parse_100104(bill, line_index, parsed):
    pass

def parse_100105(bill, line_index, parsed):
    pass

def parse_100106(bill, line_index, parsed):
    pass

def parse_100107(bill, line_index, parsed):
    pass

def parse_100200(bill, line_index, parsed):
    pass

def parse_100201(bill, line_index, parsed):
    pass

def parse_100202(bill, line_index, parsed):
    pass

def parse_100203(bill, line_index, parsed):
    pass

def parse_100204(bill, line_index, parsed):
    pass

def parse_100205(bill, line_index, parsed):
    pass

def parse_100206(bill, line_index, parsed):
    pass

def parse_100207(bill, line_index, parsed):
    pass

def parse_100208(bill, line_index, parsed):
    pass

def parse_100209(bill, line_index, parsed):
    pass

def parse_100300(bill, line_index, parsed):
    pass

def parse_100301(bill, line_index, parsed):
    pass

def parse_100302(bill, line_index, parsed):
    pass

def parse_100303(bill, line_index, parsed):
    pass

def parse_100304(bill, line_index, parsed):
    pass

def parse_100305(bill, line_index, parsed):
    pass

def parse_100306(bill, line_index, parsed):
    pass

def parse_100307(bill, line_index, parsed):
    pass

def parse_100308(bill, line_index, parsed):
    pass

def parse_100309(bill, line_index, parsed):
    pass

def parse_100310(bill, line_index, parsed):
    pass

def parse_100311(bill, line_index, parsed):
    pass

def parse_100312(bill, line_index, parsed):
    pass

def parse_100313(bill, line_index, parsed):
    pass

def parse_100400(bill, line_index, parsed):
    pass

def parse_100401(bill, line_index, parsed):
    pass

def parse_100402(bill, line_index, parsed):
    pass

def parse_100403(bill, line_index, parsed):
    pass

def parse_100404(bill, line_index, parsed):
    pass

def parse_100405(bill, line_index, parsed):
    pass

def parse_100406(bill, line_index, parsed):
    pass

def parse_100407(bill, line_index, parsed):
    pass

def parse_100408(bill, line_index, parsed):
    pass

def parse_100409(bill, line_index, parsed):
    pass

def parse_100410(bill, line_index, parsed):
    pass

def parse_100430(bill, line_index, parsed):
    pass

def parse_100500(bill, line_index, parsed):
    pass

def parse_100501(bill, line_index, parsed):
    pass

def parse_100502(bill, line_index, parsed):
    pass

def parse_100503(bill, line_index, parsed):
    pass

def parse_100600(bill, line_index, parsed):
    pass

def parse_100602(bill, line_index, parsed):
    pass

def parse_100603(bill, line_index, parsed):
    pass

def parse_100604(bill, line_index, parsed):
    pass

def parse_200000(bill, line_index, parsed):
    pass

def parse_200003(bill, line_index, parsed):
    pass

def parse_200004(bill, line_index, parsed):
    pass

def parse_200100(bill, line_index, parsed):
    pass

def parse_200101(bill, line_index, parsed):
    pass

def parse_200102(bill, line_index, parsed):
    pass

def parse_200103(bill, line_index, parsed):
    pass

def parse_200104(bill, line_index, parsed):
    pass

def parse_200105(bill, line_index, parsed):
    pass

def parse_200200(bill, line_index, parsed):
    pass

def parse_200201(bill, line_index, parsed):
    pass

def parse_200202(bill, line_index, parsed):
    pass

def parse_200203(bill, line_index, parsed):
    pass

def parse_200204(bill, line_index, parsed):
    pass

def parse_200205(bill, line_index, parsed):
    pass

def parse_200206(bill, line_index, parsed):
    pass

def parse_200207(bill, line_index, parsed):
    pass

def parse_200300(bill, line_index, parsed):
    pass

def parse_200301(bill, line_index, parsed):
    pass

def parse_200302(bill, line_index, parsed):
    pass

def parse_200303(bill, line_index, parsed):
    pass

def parse_200304(bill, line_index, parsed):
    pass

def parse_200305(bill, line_index, parsed):
    pass

def parse_200306(bill, line_index, parsed):
    pass

def parse_200307(bill, line_index, parsed):
    pass

def parse_200308(bill, line_index, parsed):
    pass

def parse_200400(bill, line_index, parsed):
    pass

def parse_200500(bill, line_index, parsed):
    pass

def parse_200503(bill, line_index, parsed):
    pass

def parse_200600(bill, line_index, parsed):
    pass

def parse_200603(bill, line_index, parsed):
    pass

def parse_300000(bill, line_index, parsed):
    pass

def parse_300100(bill, line_index, parsed):
    pass

def parse_300101(bill, line_index, parsed):
    pass

def parse_300102(bill, line_index, parsed):
    pass

def parse_300103(bill, line_index, parsed):
    pass

def parse_300104(bill, line_index, parsed):
    pass

def parse_300105(bill, line_index, parsed):
    pass

def parse_400000(bill, line_index, parsed):
    pass

def parse_400001(bill, line_index, parsed):
    pass

def parse_400002(bill, line_index, parsed):
    pass

def parse_400003(bill, line_index, parsed):
    pass

def parse_400004(bill, line_index, parsed):
    pass

def parse_400005(bill, line_index, parsed):
    pass

def parse_400006(bill, line_index, parsed):
    pass
