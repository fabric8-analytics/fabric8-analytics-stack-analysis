from util.error import error_codes


class AnalyticsException(Exception):
    def __init__(self, code):
        self.code = code

    def __str__(self):
        return "{}: {}".format(self.code.get("name"), self.code.get("msg"))
