"""Base abstract PGM class to be implemented by other classes."""

from abc import ABCMeta, abstractmethod


class AbstractPGM(object):
    """Base abstract PGM class to be implemented by other classes."""

    __metaclass__ = ABCMeta

    @classmethod
    def train(cls, _kronos_dependency_dict, _package_occurrence_df):
        """Train the Kronos model."""
        assert cls is not None  # just make checkers happy
        return

    @classmethod
    def load(cls, _data_store, _filename):
        """Load already saved Kronos."""
        assert cls is not None  # just make checkers happy
        return

    @abstractmethod
    def save(self, _data_store, _filename):
        """Save the Kronos in data_store."""
        return

    @abstractmethod
    def score(self, _evidence_dict_list):
        """Predicts the probabilites using the trained model."""
