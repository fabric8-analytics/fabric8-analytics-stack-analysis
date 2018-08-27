"""Class that represents exception that can occur during analysis."""


class AnalyticsException(Exception):
    """Class that represents exception that can occur during analysis."""

    def __init__(self, code):
        """Override the default behaviour of the Exception class."""
        self.code = code

    def __str__(self):
        """Return textual representation of the exception."""
        return "{}: {}".format(self.code.get("name"), self.code.get("msg"))
