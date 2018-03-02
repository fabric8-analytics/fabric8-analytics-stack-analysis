"""Base abstract PGM class to be implemented by other classes."""

from abc import ABCMeta, abstractmethod


class AbstractPGM(object):
    """Base abstract PGM class to be implemented by other classes."""

    __metaclass__ = ABCMeta

    @abstractmethod
    def train(cls, kronos_dependency_dict, package_occurrence_df):
        """Train the Kronos model."""
        return

    @abstractmethod
    def load(cls, data_store, filename):
        """Load already saved Kronos."""
        return

    @abstractmethod
    def save(self, data_store, filename):
        """Save the Kronos in data_store."""
        return

    @abstractmethod
    def score(self, evidence_dict_list):
        """Predicts the probabilites using the trained model."""
