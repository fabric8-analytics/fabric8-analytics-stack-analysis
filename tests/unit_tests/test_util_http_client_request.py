"""Tests for the module utils.http_client.request."""

import pytest
import requests

from utils.http_client.request import *


def test_post_function():
    """Test the post() function."""
    # an universal testing endpoint
    result = post("https://httpbin.org/post", {})
    assert result is not None


if __name__ == '__main__':
    test_post_function()
