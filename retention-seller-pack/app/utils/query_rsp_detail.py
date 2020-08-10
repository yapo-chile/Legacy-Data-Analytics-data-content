from infraestructure.conf import getConf
from utils.read_params import ReadParams

class QueryRSPDetail:
    """
    Class that store all querys
    """
    def __init__(self,
                 conf: getConf,
                 params: ReadParams) -> None:
        self.params = params
        self.conf = conf
        self.table_dest_rsp_detail =\
            "stg.retention_sellers_packs_detail"

    def delete_retention_sellers_packs_detail(self, month) -> str:
        """
        Method that returns delete query to
        execute in retention_sellers_pack_detail
        """
        command = """
            delete 
            from {table_name}  
            where month_id = {month}
        """.format(month=month,
                   table_name=self.table_dest_rsp_detail)
        return command

    def query_retention_seller_packs_detail(self) -> str:
        """
        Method return str with query related to detail
        from retention sellers pack
        """
        query = """
            select
                e.month_id,
                e.days,
                e.slots,
                coalesce(e.active_sellers_real_estate,0) as active_sellers_real_estate,
                coalesce(e.active_sellers_car,0) as active_sellers_car,
                coalesce(b.seller_real_estate_tot,0) as sellers_real_estate,
                coalesce(b.seller_car_tot,0) as sellers_car,
                coalesce(a.seller_real_estate_ret,0) as sellers_real_estate_retencion,
                coalesce(a.seller_car_ret,0) as sellers_car_retencion,
                coalesce(c.seller_real_estate_new,0) as sellers_real_estate_nuevos,
                coalesce(c.seller_car_new,0) as sellers_car_nuevos,
                coalesce(d.sellers_real_estate_fuga,0) as sellers_real_estate_fuga,
                coalesce(d.sellers_car_fuga,0) as sellers_car_fuga,
                --devengado
                coalesce(e.dev_active_real_estate,0) as dev_active_real_estate,
                coalesce(e.dev_active_car,0) as dev_active_car,
                coalesce(b.dev_real_estate_tot,0) as dev_real_estate,
                coalesce(b.dev_car_tot,0) as dev_car,
                coalesce(a.dev_real_estate_ret,0) as dev_real_estate_retencion,
                coalesce(a.dev_car_ret,0) as dev_car_retencion,
                coalesce(c.dev_real_estate_new,0) as dev_real_estate_nuevos,
                coalesce(c.dev_car_new,0) as dev_car_nuevos,
                coalesce(d.dev_real_estate_fuga,0) as dev_real_estate_fuga,
                coalesce(d.dev_car_fuga,0) dev_car_fuga
            from 
                (-- (E) Total de pack activo
                select
                    to_char('{date_from}'::date,'YYYYMM')::int as month_id,
                    t3.days,
                    t3.slots,
                    count(distinct case when t3.category = 'real_estate' then t3.email end) as active_sellers_real_estate,
                    count(distinct case when t3.category = 'car' then t3.email end) as active_sellers_car,
                    sum(distinct case when t3.category = 'real_estate' then t3.devengado end) as dev_active_real_estate,
                    sum(distinct case when t3.category = 'car' then t3.devengado end) as dev_active_car
                from 
                    (--tabla 3 usuarios que tienen algun pack activo dentro del mes en curso
                    select
                        to_char(p.date_end, 'YYYYMM')::int as date_end,
                        p.email,
                        p.category,
                        p.days,
                        p.slots,
                        (p.price/1.19)
                        /
                        (p.days)
                        *
                        ((case when p.date_end::date > ((('{date_from}'::date + interval '1 month' - interval '1 day')::date))
                        then date_part('day',(('{date_from}'::date + interval '1 month' - interval '1 day')::date))
                        else date_part('day',date_end) end) - --as deveng_2,
                        (case when p.date_start::date > '{date_from}' then date_part('day',p.date_start)
                        else 1 end) --as deveng_1
                        + 1) --as dias_deveng
                        as devengado
                    from
                        ods.packs p
                    where 1=1
                        and ((p.date_end::date >= '{date_from}')
                        and (p.date_start::date <= ('{date_from}'::date + interval '1 month' - interval '1 day')::date))
                        and p.email is not null
                        and p.days <> 0 
                    ) t3
                group by 1,2,3
                ) e
            left join
                (-- (A) Sellers que vuelven despues de un mes sin actividad y NO son nuevos
                select
                    to_char('{date_from}'::date,'YYYYMM')::int as month_id,
                    t3.days,
                    t3.slots,
                    count(distinct case when t3.category = 'real_estate' then t3.email end) as seller_real_estate_ret,
                    count(distinct case when t3.category = 'car' then t3.email end) as seller_car_ret,
                    sum(distinct case when t3.category = 'real_estate' then t3.devengado end) as dev_real_estate_ret,
                    sum(distinct case when t3.category = 'car' then t3.devengado end) as dev_car_ret
                from
                    (--tabla 3 usuarios que comprar algun pack dentro del mes en curso
                    select
                        to_char(p.date_end, 'YYYYMM')::int as date_end,
                        p.email,
                        p.category,
                        p.days,
                        p.slots,
                        (p.price/1.19)
                        /
                        (p.days)
                        *
                        ((case when p.date_end::date > ((('{date_from}'::date + interval '1 month' - interval '1 day')::date))
                        then date_part('day',(('{date_from}'::date + interval '1 month' - interval '1 day')::date))
                        else date_part('day',date_end) end) - --as deveng_2,
                        (case when p.date_start::date > '{date_from}' then date_part('day',p.date_start)
                        else 1 end) --as deveng_1
                        + 1) --as dias_deveng
                        as devengado
                    from ods.packs p
                    where 1=1
                        and ((p.date_end::date >= '{date_from}')
                        and (p.date_start::date <= ('{date_from}'::date + interval '1 month' - interval '1 day')::date))
                        and p.email is not null
                        and p.days <> 0
                        and p.email not in 
                            ( --tabla 3 usuarios que comprar algun pack dentro del mes anterior
                            select p.email
                            from ods.packs p
                            where 1 = 1
                                and ((p.date_end::date >= ('{date_from}'::Date - interval '1 month')::date )
                                and (p.date_start::date < '{date_from}'))
                                and p.email is not null
                            )
                        and p.email not in
                            (--tabla 3 usuarios no han comprado antes del mes seleccionado
                            select p.email
                            from ods.packs p
                            where 1=1
                                and p.date_start::date between '{date_from}' and ('{date_from}'::date + interval '1 month' - interval '1 day')::date -- este es el mes a evaluar
                                and p.email is not null
                                and p.email not in
                                    (--tabla 2 usuarios actiso hacia atras
                                    select distinct p.email
                                    from ods.packs p
                                    where 1=1
                                        and p.date_start::date < '{date_from}'
                                        and p.email is not null
                                    )
                            )
                    )t3
                group by 1,2,3
                ) a	on a.month_id = e.month_id and a.days = e.days and a.slots = e.slots
            left join
                (-- (B) TOTAL PACKS COMPRADOS EN EL MES
                select
                    t3.date_start as month_id,
                    t3.days,
                    t3.slots,
                    count(distinct case when t3.category = 'real_estate' then t3.email end) as seller_real_estate_tot,
                    count(distinct case when t3.category = 'car' then t3.email end) as seller_car_tot,
                    sum(distinct case when t3.category = 'real_estate' then t3.devengado end) as dev_real_estate_tot,
                    sum(distinct case when t3.category = 'car' then t3.devengado end) as dev_car_tot
                from
                    (--tabla 3 usuarios que comprar algun pack dentro del mes en curso
                    select
                        to_char(p.date_start, 'YYYYMM')::int as date_start,
                        p.email,
                        p.category,
                        p.days,
                        p.slots,
                        (p.price/1.19)
                        /
                        (p.days)
                        *
                        ((case when p.date_end::date > ((('{date_from}'::date + interval '1 month' - interval '1 day')::date))
                        then date_part('day',(('{date_from}'::date + interval '1 month' - interval '1 day')::date))
                        else date_part('day',date_end) end) - --as deveng_2,
                        (case when p.date_start::date > '{date_from}' then date_part('day',p.date_start)
                        else 1 end) --as deveng_1
                        + 1) --as dias_deveng
                        as devengado
                    from ods.packs p
                    where 1=1
                        and p.date_start::date between '{date_from}' and ('{date_from}'::date + interval '1 month' - interval '1 day')::date
                        and p.email is not null
                        and p.days <> 0
                    ) t3 
                group by 1,2,3
                ) b on e.month_id = b.month_id and e.days = b.days and e.slots = b.slots
            left join
                (-- (C) NUEVOS SELLERS QUE NUNCA HAN COMPRADO ANTES DEL MES ANALIZADO
                select
                    tot.month_id,
                    tot.days,
                    tot.slots,
                    count(distinct case when tot.category = 'real_estate' then tot.email end) as seller_real_estate_new,
                    count(distinct case when tot.category = 'car' then tot.email end) as seller_car_new,
                    sum(distinct case when tot.category = 'real_estate' then tot.devengado end) as dev_real_estate_new,
                    sum(distinct case when tot.category = 'car' then tot.devengado end) as dev_car_new
                from
                    (--tot --tabla 3 usuarios no han comprado antes del mes seleccionado
                    select
                        to_char(p.date_start, 'YYYYMM')::int as month_id,
                        p.email,
                        p.category,
                        p.days,
                        p.slots,
                        (p.price/1.19)
                        /
                        (p.days)
                        *
                        ((case when p.date_end::date > ((('{date_from}'::date + interval '1 month' - interval '1 day')::date))
                        then date_part('day',(('{date_from}'::date + interval '1 month' - interval '1 day')::date))
                        else date_part('day',date_end) end) - --as deveng_2,
                        (case when p.date_start::date > '{date_from}' then date_part('day',p.date_start)
                        else 1 end) --as deveng_1
                        + 1) --as dias_deveng
                        as devengado
                    from
                        ods.packs p
                    where 1=1
                        and p.date_start::date between '{date_from}' and ('{date_from}'::date + interval '1 month' - interval '1 day')::date -- este es el mes a evaluar
                        and p.email is not null
                        and p.days <> 0
                        and p.email not in
                            (--tabla 2 usuarios actiso hacia atras
                            select distinct p.email
                            from ods.packs p
                            where 1=1-- estas dos fechas corresponden a los 1 meses anteriores al mes de arriba
                                and p.date_start::date < '{date_from}'
                                and p.email is not null
                            )
                    ) tot
                    group by 1,2,3
                ) c on e.month_id = c.month_id and e.days = c.days and e.slots = c.slots
            left join
                (-- (D) FUGA, SELLERS QUE ESTUVIERON ACTIVOS Y NO EL MES ACTUAL
                select
                    to_char('{date_from}'::date,'YYYYMM')::int as month_id,
                    t3.days,
                    t3.slots,
                    count(distinct case when t3.category = 'real_estate' then t3.email end) as sellers_real_estate_fuga,
                    count(distinct case when t3.category = 'car' then t3.email end) as sellers_car_fuga,
                    sum(distinct case when t3.category = 'real_estate' then t3.devengado end) as dev_real_estate_fuga,
                    sum(distinct case when t3.category = 'car' then t3.devengado end) as dev_car_fuga
                from
                    (--tabla 3 usuarios que comprar algun pack dentro del mes anterior
                    select
                        to_char(p.date_end, 'YYYYMM')::int as date_end,
                        p.email,
                        p.category,
                        p.days,
                        p.slots,
                        (p.price/1.19)
                        /
                        (p.days)
                        *
                        date_part('day',('{date_from}'::date + interval '1 month' - interval '1 day')::date) as devengado
                    from
                        ods.packs p
                    where 1=1
                        and ((p.date_end::date >= ('{date_from}'::Date - interval '1 month')::date )
                        and (p.date_start::date < '{date_from}'))
                        and p.email is not null
                        and p.days <> 0
                        and p.email not in
                        (--tabla 3 usuarios que comprar algun pack dentro dsel mes en curso
                        select p.email
                        from ods.packs p
                        where 1=1
                            and ((p.date_end::date >= '{date_from}')
                            and (p.date_start::date <= ('{date_from}'::date + interval '1 month' - interval '1 day')::date))
                            and p.email is not null
                        )
                    ) t3
                group by 1,2,3
                ) d on e.month_id = d.month_id and e.days = d.days and e.slots = d.slots
        """.format(date_from=self.params.date_from)
        return query
