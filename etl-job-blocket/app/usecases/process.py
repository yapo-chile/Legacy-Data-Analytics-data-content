# pylint: disable=no-member
# utf-8
import sys
import logging
from infraestructure.athena import Athena
from infraestructure.psql import Database
from utils.query import Query
from utils.read_params import ReadParams


class Process():
    def __init__(self,
                 config,
                 params: ReadParams) -> None:
        self.config = config
        self.params = params

    def generate(self):
        