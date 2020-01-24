from infraestructure.conf import getConf


class Query:
    """
    Class that store all querys
    """
    def delete_direct_sales(self, conf: getConf) -> str:
        """
        Method that returns events of the day
        """
        command = """
                    truncate table """ + conf.db.table_network
        return command

    def delete_network_sales(self, conf: getConf) -> str:
        """
        Method that returns events of the day
        """
        command = """
                    truncate table """ + conf.db.table_direct
        return command
