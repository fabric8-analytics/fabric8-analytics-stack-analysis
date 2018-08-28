"""Tests for the TestData class."""

from evaluation_platform.uranus.src.generate_test_data import TestData


def test_test_data_class():
    """Basic test for the TestData class."""
    testData = TestData()
    # there is not any reason the class can't be instancianced
    assert testData


if __name__ == '__main__':
    test_test_data_class()
