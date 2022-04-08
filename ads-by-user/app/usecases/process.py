# pylint: disable=no-member
# utf-8
from utils.read_params import ReadParams
from usecases.ads import AdsByUser


class Process:
    def __init__(self,
                 config,
                 params: ReadParams,
                 logger) -> None:
        self.config = config
        self.params = params
        self.logger = logger

    def generate(self):
        self.ads_by_users = AdsByUser(self.config,
                                      self.params,
                                      self.logger).generate()
        return True
