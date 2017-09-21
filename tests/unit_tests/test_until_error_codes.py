from util.error import error_codes
from util.error.analytics_exception import AnalyticsException

from unittest import TestCase


class TestUtilInputValidation(TestCase):

    def test_input_invalid_error(self):
        try:
            raise AnalyticsException(error_codes.ERR_INPUT_INVALID)
        except AnalyticsException as e:
            print e
