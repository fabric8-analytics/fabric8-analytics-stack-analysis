from abc import ABCMeta, abstractmethod


class AbstractApollo(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def train(cls, data_store):
        """Preprocess the data to required gnosis and softnet format.

        :param data_store: data store where various input gnosis files are stored.

        :return: Apollo Object."""

        return

    @abstractmethod
    def load(cls, data_store, filename):
        """Loads the data to be preprocessed."""

        return

    @abstractmethod
    def save(self, data_store, filename):
        """Saves the data after preprocessing in data_store."""

        return
