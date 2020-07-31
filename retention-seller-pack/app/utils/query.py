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

    def table_dest_retention_seller_packs(self) -> str:
        """
        Method that contain dest table of retention
        seller packs process
        """
        table_name = "stg.retention_sellers_packs"
        return table_name

    def query_retention_seller_pack(self) -> str:
        """
        Method return str with query
        """
        query = """
            select
                a.month_id,
                e.active_sellers_real_estate,
                e.active_sellers_car,
                b.seller_real_estate_tot as sellers_real_estate,
                b.seller_car_tot as sellers_car,
                a.seller_real_estate_ret as sellers_real_estate_retencion,
                a.seller_car_ret as sellers_car_retencion,
                c.seller_real_estate_new as sellers_real_estate_nuevos,
                c.seller_car_new as sellers_car_nuevos,
                d.sellers_real_estate_fuga,
                d.sellers_car_fuga,
                e.dev_active_real_estate,
                e.dev_active_car,
                b.dev_real_estate_tot as dev_real_estate,
                b.dev_car_tot as dev_car,
                a.dev_real_estate_ret as dev_real_estate_retencion,
                a.dev_car_ret as dev_car_retencion,
                c.dev_real_estate_new as dev_real_estate_nuevos,
                c.dev_car_new as dev_car_nuevos,
                d.dev_real_estate_fuga,
                d.dev_car_fuga,
                e.dev_active_real_estate_30,
                e.dev_active_car_30,
                b.dev_real_estate_tot_30 as dev_real_estate_30,
                b.dev_car_tot_30 as dev_car_30,
                a.dev_real_estate_ret_30 as dev_real_estate_retencion_30,
                a.dev_car_ret_30 as dev_car_retencion_30,
                c.dev_real_estate_new_30 as dev_real_estate_nuevos_30,
                c.dev_car_new_30 as dev_car_nuevos_30
            from 
                (-- (A) Sellers que vuelven despues de un mes sin actividad y NO son nuevos
                select
                    to_char('{date_from}'::date,'YYYYMM')::int as month_id,
                    count(distinct case when t3.category = 'real_estate' then t3.email end) as seller_real_estate_ret,
                    count(distinct case when t3.category = 'car' then t3.email end) as seller_car_ret,
                    sum(case when t3.category = 'real_estate' then t3.devengado end) as dev_real_estate_ret,
                    sum(case when t3.category = 'car' then t3.devengado end) as dev_car_ret,
                    sum(case when t3.category = 'real_estate' then t3.devengado_30 end) as dev_real_estate_ret_30,
                    sum(case when t3.category = 'car' then t3.devengado_30 end) as dev_car_ret_30
                from
                    (--tabla 3 usuarios que comprar algun pack dentro del mes en curso
                    select
                        to_char(p.date_end, 'YYYYMM')::int as date_end,
                        p.email,
                        p.category,
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
                        as devengado,
                        (p.price/1.19)
                        /
                        (p.days)
                        *
                        date_part('day',(('{date_from}'::date + interval '1 month' - interval '1 day')::date))
                        as devengado_30
                    from ods.packs p
                    where 1 = 1
                        and ((p.date_end::date >= '{date_from}')
                        and (p.date_start::date <= ('{date_from}'::date + interval '1 month' - interval '1 day')::date))
                        and p.email is not null
                        and p.days <> 0
                        and p.email not in 
                            (--tabla 3 usuarios que comprar algun pack dentro del mes anterior
                            select p.email
                            from ods.packs p
                            where 1=1
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
                                where 1=1 -- estas dos fechas corresponden a los 1 meses anteriores al mes de arriba
                                    --p.date_end::date >= '{date_from}' and   --al borrar esta fecha son usuarios nuevos
                                    and p.date_start::date < '{date_from}'
                                    and p.email is not null
                                )
                            )
                    ) t3
                group by 1 
                ) a
            left join
                (-- (B) TOTAL PACKS COMPRADOS EN EL MES
                select
                    t3.date_start as month_id,
                    count(distinct case when t3.category = 'real_estate' then t3.email end) as seller_real_estate_tot,
                    count(distinct case when t3.category = 'car' then t3.email end) as seller_car_tot,
                    sum(case when t3.category = 'real_estate' then t3.devengado end) as dev_real_estate_tot,
                    sum(case when t3.category = 'car' then t3.devengado end) as dev_car_tot,
                    sum(case when t3.category = 'real_estate' then t3.devengado_30 end) as dev_real_estate_tot_30,
                    sum(case when t3.category = 'car' then t3.devengado_30 end) as dev_car_tot_30
                from
                    (--tabla 3 usuarios que comprar algun pack dentro del mes en curso
                    select
                        to_char(p.date_start, 'YYYYMM')::int as date_start,
                        p.email,
                        p.category,
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
                        as devengado,
                        (p.price/1.19)
                        /
                        (p.days)
                        *
                        date_part('day',(('{date_from}'::date + interval '1 month' - interval '1 day')::date))
                        as devengado_30
                    from ods.packs p
                    where 1=1
                        and p.date_start::date between '{date_from}' and ('{date_from}'::date + interval '1 month' - interval '1 day')::date
                        and p.email is not null
                        and p.days <> 0
                    )t3 
                group by 1
                ) b on a.month_id = b.month_id
            left join
                (-- (C) NUEVOS SELLERS QUE NUNCA HAN COMPRADO ANTES DEL MES ANALIZADO
                select
                    tot.month_id,
                    count(distinct case when tot.category = 'real_estate' then tot.email end) as seller_real_estate_new,
                    count(distinct case when tot.category = 'car' then tot.email end) as seller_car_new,
                    sum(case when tot.category = 'real_estate' then tot.devengado end) as dev_real_estate_new,
                    sum(case when tot.category = 'car' then tot.devengado end) as dev_car_new,
                    sum(case when tot.category = 'real_estate' then tot.devengado_30 end) as dev_real_estate_new_30,
                    sum(case when tot.category = 'car' then tot.devengado_30 end) as dev_car_new_30
                from 
                    (--tabla 3 usuarios no han comprado antes del mes seleccionado
                    select
                        to_char(p.date_start, 'YYYYMM')::int as month_id,
                        p.email,
                        p.category,
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
                        as devengado,
                        (p.price/1.19)
                        /
                        (p.days)
                        *
                        date_part('day',(('{date_from}'::date + interval '1 month' - interval '1 day')::date))
                        as devengado_30
                    from ods.packs p
                    where 1=1 
                        and p.date_start::date between '{date_from}' and ('{date_from}'::date + interval '1 month' - interval '1 day')::date -- este es el mes a evaluar
                        and p.email is not null
                        and p.days <> 0
                        and p.email not in
                            (--tabla 2 usuarios actiso hacia atras
                            select distinct p.email
                            from ods.packs p
                            where 1=1-- estas dos fechas corresponden a los 1 meses anteriores al mes de arriba
                                --p.date_end::date >= '{date_from}' and   --al borrar esta fecha son usuarios nuevos
                                and p.date_start::date < '{date_from}'
                                and p.email is not null
                            )
                    )tot
                group by 1
                ) c on a.month_id = c.month_id
            left join
                (-- (D) FUGA, SELLERS QUE ESTUVIERON ACTIVOS Y NO EL MES ACTUAL
                select
                    to_char('{date_from}'::date,'YYYYMM')::int as month_id,
                    count(distinct case when t3.category = 'real_estate' then t3.email end) as sellers_real_estate_fuga,
                    count(distinct case when t3.category = 'car' then t3.email end) as sellers_car_fuga,
                    sum(case when t3.category = 'real_estate' then t3.devengado end) as dev_real_estate_fuga,
                    sum(case when t3.category = 'car' then t3.devengado end) as dev_car_fuga
                from
                    (--tabla 3 usuarios que comprar algun pack dentro del mes anterior
                    select
                        to_char(p.date_end, 'YYYYMM')::int as date_end,
                        p.email,
                        p.category,
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
                        (--tabla 3 usuarios que comprar algun pack dentro del mes en curso
                        select
                            p.email
                        from ods.packs p
                        where 1=1
                            and ((p.date_end::date >= '{date_from}')
                            and (p.date_start::date <= ('{date_from}'::date + interval '1 month' - interval '1 day')::date))
                            and p.email is not null
                        )
                    )t3
                group by 1
                ) d on d.month_id = a.month_id
            left join
                (-- (E) Total de pack activo
                select
                    to_char('{date_from}'::date,'YYYYMM')::int as month_id,
                    count(distinct case when t3.category = 'real_estate' then t3.email end) as active_sellers_real_estate,
                    count(distinct case when t3.category = 'car' then t3.email end) as active_sellers_car,
                    sum(case when t3.category = 'real_estate' then t3.devengado end) as dev_active_real_estate,
                    sum(case when t3.category = 'car' then t3.devengado end) as dev_active_car,
                    sum(case when t3.category = 'real_estate' then t3.devengado_30 end) as dev_active_real_estate_30,
                    sum(case when t3.category = 'car' then t3.devengado_30 end) as dev_active_car_30
                from
                    (--tabla 3 usuarios que tienen algun pack activo dentro del mes en curso
                    select
                        to_char(p.date_end, 'YYYYMM')::int as date_end,
                        p.email,
                        p.category,
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
                        as devengado,
                        (p.price/1.19)
                        /
                        (p.days)
                        *
                        date_part('day',(('{date_from}'::date + interval '1 month' - interval '1 day')::date))
                        as devengado_30
                    from
                        ods.packs p
                    where 1=1 
                        and ((p.date_end::date >= '{date_from}')
                        and (p.date_start::date <= ('{date_from}'::date + interval '1 month' - interval '1 day')::date))
                        and p.email is not null
                        and p.days <> 0
                    ) t3
                group by 1
                ) e on e.month_id = a.month_id 
        """.format(date_from=self.params.date_from)
        return query

    def delete_retention_sellers_packs(self) -> str:
        """
        Method that returns query of records of month
        """
        command = """
            delete 
            from {table} 
            where ((month_id*100+01)::varchar)::date = '{date_from}'
        """.format(date_from=self.params.date_from,
                   table=self.table_dest_retention_seller_packs())
        return command

    def table_dest_retention_seller_packs_detail(self) -> str:
        """
        Method
        """
        table_name = "stg.retention_sellers_pack_detail"
        return table_name

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
                   table_name=self.table_dest_retention_seller_packs_detail())
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
