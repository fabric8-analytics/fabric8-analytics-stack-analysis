from abc import ABCMeta, abstractmethod


class AbstractGnosis(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def train(cls, data_store):
        """
        Trains/De-dupes Gnosis from gnosis files, which should in the following json format:

        :param data_store: data store where various input gnosis files are stored
        :return: Gnosis object
        """
        return

    @abstractmethod
    def load(cls, data_store, filename):
        """
        Loads already saved Gnosis
        """
        return

    @abstractmethod
    def save(self, data_store, filename):
        """
        Saves the Gnosis in data_store
        """
        return

