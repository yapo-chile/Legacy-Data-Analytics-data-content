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

    def query_sellers_pack_leak(self) -> str:
        """
        Method that return query to get sellers pack leak
        """
        query = """
            select
                t3.*,
                pr.product_name,
                sa."name",
                sa.gender,
                sa.phone,
                sa.email
            from (--tabla 3 usuarios que tiene un pack activo el mes anterior
                select
                    to_char(p.date_end, 'YYYYMM')::int as date_end,
                    p.seller_id_fk,
                    p.category,
                    p.product_id
                from ods.packs p
                where 1=1
                    and ((p.date_end::date >= ('{date_to}'::Date - interval '1 month')::date )
                    and (p.date_start::date < '{date_to}'))
                    and p.seller_id_fk is not null
                    and p.seller_id_fk not in
                    (--tabla 3 usuarios que comprar algun pack dentro del mes en curso
                    select
                        p.seller_id_fk
                    from ods.packs p
                    where 1=1
                        and ((p.date_end::date >= '{date_to}')
                        and (p.date_start::date <= ('{date_to}'::date + interval '1 month' - interval '1 day')::date))
                        and p.seller_id_fk is not null
                )
            )t3
            inner join ods.seller s on s.seller_id_pk = t3.seller_id_fk
            inner join ods.social_accounts sa on s.email = sa.email
            left join ods.product pr on pr.product_id_nk::int = t3.product_id
            group by 1,2,3,4,5,6,7,8,9
        """.format(date_to=self.params.date_to)
        return query
