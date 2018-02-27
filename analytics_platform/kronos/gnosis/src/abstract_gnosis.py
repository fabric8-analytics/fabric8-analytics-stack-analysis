"""Base abstract Gnosis class to be implemented by other classes."""

from abc import ABCMeta, abstractmethod


class AbstractGnosis(object):
    """Base abstract Gnosis class to be implemented by other classes."""

    __metaclass__ = ABCMeta

    @abstractmethod
    def train(cls, data_store):
        """Train/De-dupe Gnosis from gnosis files.

        The files should be in the following json format:

        :param data_store: data store where various input gnosis files are stored.

        :return: Gnosis object.
        """
        return

    @abstractmethod
    def load(cls, data_store, filename):
        """Load already saved Gnosis."""
        return

    @abstractmethod
    def save(self, data_store, filename):
        """Save the Gnosis in data_store."""
        return
