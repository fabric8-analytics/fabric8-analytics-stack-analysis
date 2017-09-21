from util.error import error_codes
from util.error.analytics_exception import AnalyticsException


def test_input_invalid_error():
    try:
        raise AnalyticsException(error_codes.ERR_INPUT_INVALID)
    except AnalyticsException as e:
        print e
