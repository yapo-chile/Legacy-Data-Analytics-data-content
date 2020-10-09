# pylint: disable=no-member
# utf-8
from utils.read_params import ReadParams
from usecases.ads_to_stg import AdsToStg
from usecases.ads_to_ods import AdsToOds

class Process():
    def __init__(self,
                 config,
                 params: ReadParams,
                 logger) -> None:
        self.config = config
        self.params = params
        self.logger = logger

    def generate(self):
        # First step: Getting ads from blocket db data to stg schema into dwh
        self.ads_to_stg = AdsToStg(self.config,
                                   self.params,
                                   self.logger).generate()

        # Second step: Getting ads from stg schema to ods schema into dwh
        self.ads_to_ods = AdsToOds(self.config,
                                   self.params,
                                   self.logger).generate()
