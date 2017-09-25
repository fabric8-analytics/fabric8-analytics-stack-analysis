from util.error import error_codes
from util.error.analytics_exception import AnalyticsException
from unittest import TestCase


class UtilExceptionChecker(TestCase):

    def test_input_invalid_error(self):
        try:
            raise AnalyticsException(error_codes.ERR_INPUT_INVALID)
        except AnalyticsException as e:
            self.assertTrue(str(e) == "ERR_INPUT_INVALID : Input is invalid.")

    def test_model_unavailable_error(self):
        try:
            raise AnalyticsException(error_codes.ERR_MODEL_NOT_AVAILABLE)
        except AnalyticsException as e:
            self.assertTrue(str(
                e) == "ERR_MODEL_NOT_AVAILABLE: Model does not seem to be available! It should be either trained or loaded before scoring.")
