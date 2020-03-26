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

    def query_update_stg(self,
                         account_id: str,
                         date_start: str,
                         date_end: str,
                         slots: str,
                         price: str,
                         doc_num: str) -> str:
        command = """
        select 
        	sp.account_id,
        	sp.category,
        	sp.date_start::timestamp,
        	sp.date_end::timestamp,
        	sp.days,
        	sp.slots,
        	sp.product_id,
        	sp.price,
        	sp.doc_num,
        	sp.tipo_pack,
        	sp.email
        from stg.packs sp
        where 
            sp.account_id = {0}
            and sp.date_start::date = '{1}'::date
            and sp.date_end::date = '{2}'::date
            and sp.slots = {3}
            and sp.price = {4}
            and sp.doc_num = {5}
        """.format(account_id,
                   date_start,
                   date_end,
                   slots,
                   price,
                   doc_num)
        return command

    def update_stg_packs(self,
            	         category: str,
        	             days: str,
        	             product_id: str,
        	             tipo_pack: str,
        	             email: str,
                         account_id: str,
                         date_start: str,
                         date_end: str,
                         slots: str,
                         price: str,
                         doc_num: str) -> str:
        """
        Method that returns events of the day
        """
        command = """
        update stg.packs
        set 
        	category = '{0}',
        	days = {1},
        	product_id = {2},
        	tipo_pack = '{3}',
        	email = '{4}'
        from stg.packs sp
        where 
            sp.account_id = {5}
            and sp.date_start::date = '{6}'::date
            and sp.date_end::date = '{7}'::date
            and sp.slots = {8}
            and sp.price = {9}
            and sp.doc_num = {10}
        """.format(category,
        	       days,
        	       product_id,
        	       tipo_pack,
        	       email,
                   account_id,
                   date_start,
                   date_end,
                   slots,
                   price,
                   doc_num)
        return command

    def delete_pivot(self) -> str:
        """
        Method that returns events of the day
        """
        command = """
                    truncate table  """ + self.conf.db.table_pivot

        return command
