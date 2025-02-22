# pylint: disable=no-member
# utf-8
import logging
from utils.read_params import ReadParams
from usecases.revparams import RevParams



class Process():
    def __init__(self,
                 config,
                 params: ReadParams,
                 logger) -> None:
        self.config = config
        self.params = params
        self.logger = logger

    def generate(self):
        self.rev_params = RevParams(self.config,
                                    self.params,
                                    self.logger).generate()
