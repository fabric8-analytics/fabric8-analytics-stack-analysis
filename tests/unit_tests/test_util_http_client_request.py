"""Tests for the module utils.http_client.request."""

from util.http_client.request import post


def test_post_function():
    """Test the post() function."""
    # an universal testing endpoint
    result = post("https://httpbin.org/post", {})
    assert result is not None


if __name__ == '__main__':
    test_post_function()
