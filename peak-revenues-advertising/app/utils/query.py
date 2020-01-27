from infraestructure.conf import getConf


class Query:
    """
    Class that store all querys
    """
    def __init__(self, config: getConf):
        self.config = config

    def delete_direct_sales(self) -> str:
        """
        Method that returns events of the day
        """
        command = """
                    truncate table """ + self.config.db.table_network
        return command

    def delete_network_sales(self) -> str:
        """
        Method that returns events of the day
        """
        command = """
                    truncate table """ + self.config.db.table_direct
        return command
