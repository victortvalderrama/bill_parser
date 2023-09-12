from datetime import datetime
from functools import partial
from collections import namedtuple
from models import LineError
from ioutils import file_stream_reader
from helpers import *
import re

# SECTION 10000

def parse_100000(bill, line_index, parsed):
    # undefined_line(line_index, parsed)
    print(parsed.predicate[28:])

def parse_100001(bill, line_index, parsed):
    pass

def parse_100002(bill, line_index, parsed):
    pass

def parse_100003(bill, line_index, parsed):
    pass

def parse_100004(bill, line_index, parsed):
    pass

def parse_100005(bill, line_index, parsed):
    pass

def parse_100006(bill, line_index, parsed):
    pass

def parse_100007(bill, line_index, parsed):
    pass

def parse_100008(bill, line_index, parsed):
    pass

def parse_100009(bill, line_index, parsed):
    pass

def parse_100010(bill, line_index, parsed):
    pass

def parse_100011(bill, line_index, parsed):
    pass

def parse_100012(bill, line_index, parsed):
    pass

def parse_100013(bill, line_index, parsed):
    pass

def parse_100014(bill, line_index, parsed):
    pass

def parse_100015(bill, line_index, parsed):
    pass

def parse_100016(bill, line_index, parsed):
    pass

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
