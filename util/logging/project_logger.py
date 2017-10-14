import daiquiri
import logging

daiquiri.setup(level=logging.DEBUG)
logger = daiquiri.getLogger(__name__)
