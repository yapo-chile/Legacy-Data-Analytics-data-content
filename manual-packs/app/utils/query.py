from infraestructure.conf import getConf
from utils.read_params import ReadParams


class Query:
    """
    Class that store all querys
    """
    def __init__(self,
                 conf: getConf,
                 params: ReadParams) -> None:
        self.params = params
        self.conf = conf

    def query_product(self) -> str:
        """
        Method return str with query
        """
        query = """
                select
                    product_id_nk,
                    product_name
                from ods.product
                where date_to::date >= '{0}'::date;
            """.format(self.params.get_date_from())
        return query

    def query_pivot(self) -> str:
        """
        Method return str with query
        """
        query = """
        select
            account_id,
            category,
            date_start::timestamp,
            date_end::timestamp,
            days,
            slots,
            product_id,
            price,
            doc_num,
            tipo_pack,
            email
        from {0}
        """.format(self.conf.db.table_pivot)
        return query

    def delete_pivot(self) -> str:
        """
        Method that returns events of the day
        """
        command = """
                    truncate table  """ + self.conf.db.table_pivot

        return command
