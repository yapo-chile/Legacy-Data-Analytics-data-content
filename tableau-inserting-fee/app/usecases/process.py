# pylint: disable=no-member
# utf-8
from infraestructure.psql import Database
from utils.read_params import ReadParams
from usecases.inserting_fee import TableauInsertingFee


class Process():
    def __init__(self,
                 config,
                 params: ReadParams,
                 logger) -> None:
        self.config = config
        self.params = params
        self.logger = logger

    def generate(self):
        self.logger.info('Starting Process')
        self.big_seller_params = TableauInsertingFee(self.config,
                                                     self.params,
                                                     self.logger).generate()
        self.logger.info('END - OK')