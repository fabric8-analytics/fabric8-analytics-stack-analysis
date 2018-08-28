"""Base abstract Gnosis class to be implemented by other classes."""

from abc import ABCMeta, abstractmethod


class AbstractGnosis(object):
    """Base abstract Gnosis class to be implemented by other classes."""

    __metaclass__ = ABCMeta

    @classmethod
    def train(cls, _data_store):
        """Train/De-dupe Gnosis from gnosis files.

        The files should be in the following json format:

        :param data_store: data store where various input gnosis files are stored.

        :return: Gnosis object.
        """
        assert cls is not None  # just make checkers happy
        return

    @classmethod
    def load(cls, _data_store, _filename):
        """Load already saved Gnosis."""
        assert cls is not None  # just make checkers happy
        return

    @abstractmethod
    def save(self, _data_store, _filename):
        """Save the Gnosis in data_store."""
        return
