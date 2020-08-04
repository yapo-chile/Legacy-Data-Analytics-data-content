# pylint: disable=no-member
# utf-8
from usecases.ads_to_stg import AdsToStg
from usecases.ads_to_ods import AdsToOds

class Process(AdsToStg, AdsToOds):
    def __init__(self,
                 config,
                 params) -> None:
        self.config = config
        self.params = params

    def generate(self):
        self.save_to_stg_ad()
        self.save_to_stg_ad_approved()
        self.save_to_stg_ad_deleted()
        self.save_to_ods_ad()
