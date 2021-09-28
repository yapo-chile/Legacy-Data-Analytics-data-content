# pylint: disable=no-member
# utf-8
from infraestructure.psql import Database
from utils.query import TableauKpiFraudeQuery
from utils.read_params import ReadParams


class TableauKpiFraude(TableauKpiFraudeQuery):
    def __init__(self,
                 config,
                 params: ReadParams,
                 logger) -> None:
        self.config = config
        self.params = params
        self.logger = logger

    # Query data from data warehouse
    @property
    def dwh_kpi_fraude(self):
        return self.__dwh_kpi_fraude

    @dwh_kpi_fraude.setter
    def dwh_kpi_fraude(self, config):
        db_source = Database(conf=config)
        dwh_kpi_fraude = db_source.select_to_dict(self.select_kpi_fraude())
        db_source.close_connection()
        self.__dwh_kpi_fraude = dwh_kpi_fraude

    def insert_to_table(self):
        dwh = Database(conf=self.config.db)
        dwh.execute_command(self.truncate_table())    
        dwh.insert_copy(self.cleaned_data, "dm_analysis", "temp_tableau_kpi_fraud")

    def generate(self):
        self.dwh_kpi_fraude = self.config.db
        self.cleaned_data = self.dwh_kpi_fraude
        for column in ["main_ad_id"]:
            self.cleaned_data[column] = self.cleaned_data[column].astype('Int64')

        self.logger.info("First records as evidence")
        self.logger.info(self.cleaned_data.head())
        self.insert_to_table()
        self.logger.info("Kpi Fraude was succesfully saved")
        return True



