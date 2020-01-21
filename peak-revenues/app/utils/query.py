from utils.read_params import ReadParams


class Query:
    """
    Class that store all querys
    """
    def query_premium_products(self, params: ReadParams) -> str:
        """
        Method return a string with query
        """
        query = """
            select
                date_id,
                vertical,
                'Premium Products' as revenue_type,
                revenues_pp as amount
            from
            (select 
                rpp.date_id,
                rpp.vertical,
                rpp.revenues as revenues_pp
            from 
                ods.dim_calendar dc
            left join
                (select
                    date_id,
                    vertical,
                    sum(price_pro/1.19)::int revenues
                from 
                    dm.revenues_pp_analysis
                group by 1,2) rpp using(date_id)
            where
                date_id::date
                between ('""" + params.get_last_year_week(-374) + """'::date)
                and '""" + params.get_last_year_week(365) + """'::date) a
            order by a.date_id
            """
        return query

    def query_packs_real_estate(self, params: ReadParams) -> str:
        """
        Method return a string with query
        """
        query = """
        select
        x.date_id,
        'Real Estate' as vertical,
        'Packs' as revenue_type,
        (x.pack_inmo_sin_iva + y.pack_inmo_manual_sin_iva)::int as amount
        from
            ( -- x: Packs Online
            select
                d.date_id,
                sum(case when d.product_name like '%Pack Inmo%'
                    then d.price_pro end) as pack_inmo_sin_iva
            from
                ( --d: Nombre del producto
                select
                    c.date_id,
                    p.product_name,
                    price_pro/1.19 as price_pro
                from
                    ( --c: calendar
                    select
                        dc.date_id,
                        b.*
                    from
                        ods.dim_calendar dc
                    left join
                        ( --b: Dias donde termina, precio devengado
                        select
                            a.*,
                            a.payment_date::date + dias_duracion -1
                                as end_date,
                            a.price * 1.0 / dias_duracion
                                as price_pro
                        from
                            ( --a: Data Inicial
                            select
                                po.payment_date::date,
                                po.product_id_fk,
                                case
                                    when p.product_id_pk in
                                    (1, 140, 141, 174, 15) then 1
                                    when p.product_id_pk in
                                    (3, 8, 106, 105, 9, 10, 11, 16, 17) then 7
                                    when p.product_id_pk in
                                    (4) then 31
                                    when p.product_id_pk in
                                    (5) then 93
                                    when p.product_id_pk in
                                    (6) then 186
                                    when p.product_id_pk in
                                    (7) then 372
                                    when p.product_id_pk in
                                    (2, 175, 107, 175, 12, 13, 14) then 30
                                    when p.product_name like '%Monthly%'
                                    and p.product_name like '%Pack%'
                                        then 31
                                    when p.product_name like '%Quarterly%'
                                    and p.product_name like '%Pack%'
                                        then 93
                                    when p.product_name like '%Biannual%'
                                    and p.product_name like '%Pack%'
                                        then 186
                                    when p.product_name like '%Annual%'
                                    and p.product_name like '%Pack%'
                                        then 372
                                    when p.product_name like '%mensual%'
                                        and p.product_name like '%Pack%'
                                        then 31
                                    when p.product_name like '%trimestral%'
                                        and p.product_name like '%Pack%'
                                        then 93
                                    when p.product_name like '%semestral%'
                                        and p.product_name like '%Pack%'
                                        then 186
                                    when p.product_name like '%anual%'
                                        and p.product_name like '%Pack%'
                                        then 372
                                    when p.product_id_pk in (18)
                                        then pod.num_days::int
                                    else 1
                                end as dias_duracion,
                                po.price
                            from
                                ods.product_order po
                            inner join ods.product p on
                                po.product_id_fk = p.product_id_pk
                            left join ods.product_order_detail pod on
                                pod.purchase_detail_id_nk::int =
                                    po.purchase_detail_id_nk::int
                            where
                                po.payment_date::date between 
                            '""" + params.get_inital_day(-374) + """'::date
                            and '""" + params.get_date_from() +"""'::date
                            and po.status in('confirmed',
                                                'paid',
                                                'sent',
                                                'failed')
                            and po.product_id_fk <> 19 
                            )a
                        )b on dc.date_id::date between b.payment_date::date
                        and end_date::date
                        where
                        dc.date_id
                        between '""" + params.get_inital_day(-374) + """'::date
                        and '""" + params.get_last_year_week(365) + """'::date
                    )c
                    left join ods.product p on
                        c.product_id_fk = p.product_id_pk
                    where
                        p.product_name like '%Pack Inmo%'
                )d
                group by 1) x
            left join 
                ( -- y: Packs Offline
                select
                    date_id,
                    sum(case when category_pack = 'Pack Autos'
                    then price_pro end) as pack_autos_manual_sin_iva,
                    sum(case when category_pack = 'Pack Inmo'
                    then price_pro end) as pack_inmo_manual_sin_iva
                from
                    ( --c
                    select
                        dc.date_id,
                        b.*
                    from
                        ods.dim_calendar dc
                    left join
                        ( --b
                        select
                            a.*,
                            a.price_sin_iva*1.0 / dias_duracion as price_pro
                        from
                            ( --a
                            select
                                date_start::date,
                                tipo_pack,
                                case
                                    when category = 'car' then 'Pack Autos'
                                    else 'Pack Inmo'
                                end as category_pack,
                                ((days*1.0 / 30)::int)::varchar(4) as meses,
                                date_end::date as end_date,
                                days as dias_duracion,
                                price / 1.19 as price_sin_iva
                            from
                                ods.packs
                            where
                                tipo_pack not in ('Pack Online')
                                and price > 0
                                and days > 0
                                and date_start::date >= '2016-05-01'
                            )a 
                        )b on
                        dc.date_id::date between b.date_start::date
                        and end_date::date
                    where
                    dc.date_id::date
                    between '""" + params.get_inital_day(-374) + """'::date
                    and '""" + params.get_last_year_week(365) + """'::date )c
                    group by 1) y on x.date_id = y.date_id 
            """
        return query

    def query_insertion_fee_motors(self, params: ReadParams) -> str:
        """
        Method return a string with query
        """
        query = """
            select
                t.payment_date as date_id,
                'Motor' as vertical,
                'Insertion Fee' as revenue_type,
                t.price::int as amount
            from
            (--t
            select
                po.payment_date::date,
                po.product_id_fk,
                sum(po.price)/1.19 as price
            from
                ods.product_order po
            where
                po.payment_date::date
                between '""" + params.get_inital_day(-374) + """'::date
                and '""" + params.get_last_year_week(365) + """'::date
                and po.status in ('confirmed','sent', 'paid', 'failed')
                    and po.product_id_fk <> 19
            group by
                1,2
            )t
            inner join
                ods.product p on p.product_id_pk = t.product_id_fk
            where
                p.product_name = 'Insertion Fee Auto'
            """
        return query

    def query_insertion_fee_real_estate(self, params: ReadParams) -> str:
        """
        Method return a string with query
        """
        query = """
            select
                t.payment_date as date_id,
                'Real Estate' as vertical,
                'Insertion Fee' as revenue_type,
                t.price::int as amount
            from
            (--t
            select
                po.payment_date::date,
                po.product_id_fk,
                sum(po.price)/1.19 as price
            from
                ods.product_order po
            where
                po.payment_date::date
                between '""" + params.get_inital_day(-374) + """'::date
                and '""" + params.get_last_year_week(365) + """'::date
                and po.status in ('confirmed','sent', 'paid', 'failed')
                and po.product_id_fk <> 19
            group by
                1,2
            )t
            inner join
                ods.product p on p.product_id_pk = t.product_id_fk
            where
                p.product_name = 'Insertion Fee Inmo'
            """
        return query

    def query_insertion_fee_jobs(self, params: ReadParams) -> str:
        """
        Method return a string with query
        """
        query = """
            select
                t.payment_date as date_id,
                'Jobs' as vertical,
                'Insertion Fee' as revenue_type,
                t.price::int as amount
            from
            (--t
            select
                po.payment_date::date,
                po.product_id_fk,
                sum(po.price)/1.19 as price
            from
                ods.product_order po
            where
                po.payment_date::date
                between '""" + params.get_inital_day(-374) + """'::date
                and '""" + params.get_last_year_week(365) + """'::date
                and po.status in ('confirmed','sent', 'paid', 'failed')
                    and po.product_id_fk <> 19
            group by
                1,2
            ) t
            inner join
                ods.product p on p.product_id_pk = t.product_id_fk
            where
                p.product_name = 'Insertion Fee'
            """
        return query

    def query_packs_motors(self, params: ReadParams) -> str:
        """
        Method return a string with query
        """
        query = """
        select
        x.date_id,
        'Motor' as vertical,
        'Packs' as revenue_type,
        (x.pack_auto_sin_iva + y.pack_auto_manual_sin_iva)::int as amount
        from
        ( -- x: Packs Online
        select
            d.date_id,
            sum(case when d.product_name like '%Pack Auto%'
                then d.price_pro end) as pack_auto_sin_iva
        from
            ( --d: Nombre del producto
            select
                c.date_id,
                p.product_name,
                price_pro/1.19 as price_pro
            from
                ( --c: calendar
                select
                    dc.date_id,
                    b.*
                from
                    ods.dim_calendar dc
                left join
                    ( --b: Dias donde termina, precio devengado
                    select
                        a.*,
                        a.payment_date::date + dias_duracion -1
                            as end_date,
                        a.price * 1.0 / dias_duracion
                            as price_pro
                    from
                        ( --a: Data Inicial
                        select
                            po.payment_date::date,
                            po.product_id_fk,
                            case
                                when p.product_id_pk in
                                    (1, 140, 141, 174, 15) then 1
                                when p.product_id_pk in
                                    (3, 8, 106, 105, 9, 10, 11, 16, 17) then 7
                                when p.product_id_pk in
                                    (4) then 31
                                when p.product_id_pk in
                                    (5) then 93
                                when p.product_id_pk in
                                    (6) then 186
                                when p.product_id_pk in
                                    (7) then 372
                                when p.product_id_pk in
                                    (2, 175, 107, 175, 12, 13, 14) then 30
                                when p.product_name like '%Monthly%'
                                    and p.product_name like '%Pack%'
                                    then 31
                                when p.product_name like '%Quarterly%' 
                                    and p.product_name like '%Pack%'
                                    then 93
                                when p.product_name like '%Biannual%'
                                    and p.product_name like '%Pack%'
                                    then 186
                                when p.product_name like '%Annual%'
                                    and p.product_name like '%Pack%'
                                    then 372
                                when p.product_name like '%mensual%'
                                    and p.product_name like '%Pack%'
                                    then 31
                                when p.product_name like '%trimestral%'
                                    and p.product_name like '%Pack%'
                                    then 93
                                when p.product_name like '%semestral%'
                                    and p.product_name like '%Pack%'
                                    then 186
                                when p.product_name like '%anual%'
                                    and p.product_name like '%Pack%'
                                    then 372
                                when p.product_id_pk in
                                    (18) then pod.num_days::int
                                else 1
                            end as dias_duracion,
                            po.price
                        from
                            ods.product_order po
                        inner join ods.product p on
                            po.product_id_fk = p.product_id_pk
                        left join ods.product_order_detail pod on
                            pod.purchase_detail_id_nk::int =
                                po.purchase_detail_id_nk::int
                        where
                        po.payment_date::date
                        between '""" + params.get_inital_day(-374) + """'::date
                        and '""" + params.get_last_year_week(365) + """'::date
                        and po.status in('confirmed', 'paid', 'sent', 'failed')
                        and po.product_id_fk <> 19 
                        )a
                    )b on dc.date_id::date
                    between b.payment_date::date
                    and end_date::date
                    where
                    dc.date_id
                    between '""" + params.get_inital_day(-374) + """'::date
                    and '""" + params.get_last_year_week(365) + """'::date
                )c
                left join ods.product p on
                    c.product_id_fk = p.product_id_pk
                where
                    p.product_name like '%Pack Auto%'
            )d
            group by 1) x
        left join 
            ( -- y: Packs Offline
            select
                date_id,
                sum(case when category_pack = 'Pack Autos'
                    then price_pro end) as pack_auto_manual_sin_iva,
                sum(case when category_pack = 'Pack Inmo'
                    then price_pro end) as pack_inmo_manual_sin_iva
            from
                ( --c
                select
                    dc.date_id,
                    b.*
                from
                    ods.dim_calendar dc
                left join
                    ( --b
                    select
                        a.*,
                        a.price_sin_iva*1.0 / dias_duracion as price_pro
                    from
                        ( --a
                        select
                            date_start::date,
                            tipo_pack,
                            case
                                when category = 'car' then 'Pack Autos'
                                else 'Pack Inmo'
                            end as category_pack,
                            ((days*1.0 / 30)::int)::varchar(4) as meses,
                            date_end::date as end_date,
                            days as dias_duracion,
                            price / 1.19 as price_sin_iva
                        from
                            ods.packs
                        where
                            tipo_pack not in ('Pack Online')
                            and price > 0
                            and days > 0
                            and date_start::date >= '2016-05-01'
                        )a 
                    )b on
                    dc.date_id::date between b.date_start::date and end_date::date
                where
                dc.date_id::date
                between '""" + params.get_inital_day(-374) + """'::date
                and '""" + params.get_last_year_week(365) + """'::date )c
                group by 1) y on x.date_id = y.date_id 
            """
        return query

    def truncate_table(self, params: ReadParams) -> str:
        """
        Method that returns events of the day
        """
        command = """
                    truncate table dm_peak.revenues """

        return command
