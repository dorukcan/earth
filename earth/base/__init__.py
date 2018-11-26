from .database import *
from .io_models import *
from .reader import *
from .register import *
from .register_models import *
from .writer import *


class Earth:
    def __init__(self):
        self.db = Database()

        self.writer = Writer()
        self.reader = Reader()
