import pytest
from clarogt_validacion.models import FixedBill, Line
from clarogt_validacion import line_parsers_fixed
from functools import partial

class TestLineParsers:

    def setup_method(self):
        self.bill = FixedBill(start_line=1)
        self.base_line = partial(Line,
            section_index="1000000",
            section="99",
            sub_section=None,
            sub_sub_section=None,
        )

    def test_parse_0100100_working(self):
        # valid line.
        good_predicate = "                                                                EMISION:   14/JUL/2023SN N     002334477"  # noqa:E501
        line_parsers_fixed.parse_0100100(
            self.bill,
            0,
            self.base_line(
                predicate=good_predicate
            )
        )
        assert len(self.bill.errors) == 0

    def test_parse_0100100_bad_emitter(self):
        # bad emiiter.
        bad_emitter = "                                                                BADCHAREMISION:   14/JUL/2023SN N     002334477"
        line_parsers_fixed.parse_0100100(
            self.bill,
            0,
            self.base_line(
                predicate=bad_emitter
            )
        )
        assert len(self.bill.errors) == 2
        import pprint
        pprint.pprint(self.bill.errors)
        assert False
