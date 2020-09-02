# pylint: disable=no-member
# utf-8


class AdParamsCarsQuery:

    def dwh_ad_params(self) -> str:
        return "select * from ods.ads_cars_params"

    def blocket_ad_params(self) -> str:
        """
        Method return str with query
        """
        query = """
            select
                ad_id::int as ad_id_nk,
                (select value from ad_params where "name" = 'regdate' and ad_id = ads.ad_id)::int as car_year,
                (select value from ad_params where "name" = 'cartype' and ad_id = ads.ad_id)::int as car_type,
                (select value from ad_params where "name" = 'brand' and ad_id = ads.ad_id)::int as brand,
                (select value from ad_params where "name" = 'model' and ad_id = ads.ad_id)::int as model,
                (select value from ad_params where "name" = 'version' and ad_id = ads.ad_id)::int as "version",
                (select value from ad_params where "name" = 'pack_status' and ad_id = ads.ad_id)::text as pack_status,
                (select upper(value) from ad_params where "name" = 'plates' and ad_id = ads.ad_id)::varchar(10) as plates,
                (select value from ad_params where "name" = 'mileage' and ad_id = ads.ad_id)::int as mileage,
                (select value from ad_params where "name" = 'cubiccms' and ad_id = ads.ad_id)::int as cubiccms,
                (select value from ad_params where "name" = 'fuel' and ad_id = ads.ad_id)::int as fuel,
                (select value from ad_params where "name" = 'gearbox' and ad_id = ads.ad_id)::int as gearbox
            from
                ads
            where
                modified_at::date between '{0}' and '{1}'
                and category in (2020,2040,2060)
            union all select
                ad_id::int as ad_id_nk,
                (select value from blocket_{2}.ad_params where "name" = 'regdate' and ad_id = ads.ad_id)::int as car_year,
                (select value from blocket_{2}.ad_params where "name" = 'cartype' and ad_id = ads.ad_id)::int as car_type,
                (select value from blocket_{2}.ad_params where "name" = 'brand' and ad_id = ads.ad_id)::int as brand,
                (select value from blocket_{2}.ad_params where "name" = 'model' and ad_id = ads.ad_id)::int as model,
                (select value from blocket_{2}.ad_params where "name" = 'version' and ad_id = ads.ad_id)::int as "version",
                (select value from blocket_{2}.ad_params where "name" = 'pack_status' and ad_id = ads.ad_id)::text as pack_status,
                (select upper(value) from blocket_{2}.ad_params where "name" = 'plates' and ad_id = ads.ad_id)::varchar(10) as plates,
                (select value from blocket_{2}.ad_params where "name" = 'mileage' and ad_id = ads.ad_id)::int as mileage,
                (select value from blocket_{2}.ad_params where "name" = 'cubiccms' and ad_id = ads.ad_id)::int as cubiccms,
                (select value from blocket_{2}.ad_params where "name" = 'fuel' and ad_id = ads.ad_id)::int as fuel,
                (select value from blocket_{2}.ad_params where "name" = 'gearbox' and ad_id = ads.ad_id)::int as gearbox
            from
                blocket_{2}.ads
            where
                modified_at::date between '{0}' and '{1}'
                and category in (2020,2040,2060)
            union all select
                ad_id::int as ad_id_nk,
                (select value from blocket_{3}.ad_params where "name" = 'regdate' and ad_id = ads.ad_id)::int as car_year,
                (select value from blocket_{3}.ad_params where "name" = 'cartype' and ad_id = ads.ad_id)::int as car_type,
                (select value from blocket_{3}.ad_params where "name" = 'brand' and ad_id = ads.ad_id)::int as brand,
                (select value from blocket_{3}.ad_params where "name" = 'model' and ad_id = ads.ad_id)::int as model,
                (select value from blocket_{3}.ad_params where "name" = 'version' and ad_id = ads.ad_id)::int as "version",
                (select value from blocket_{3}.ad_params where "name" = 'pack_status' and ad_id = ads.ad_id)::text as pack_status,
                (select upper(value) from blocket_{3}.ad_params where "name" = 'plates' and ad_id = ads.ad_id)::varchar(10) as plates,
                (select value from blocket_{3}.ad_params where "name" = 'mileage' and ad_id = ads.ad_id)::int as mileage,
                (select value from blocket_{3}.ad_params where "name" = 'cubiccms' and ad_id = ads.ad_id)::int as cubiccms,
                (select value from blocket_{3}.ad_params where "name" = 'fuel' and ad_id = ads.ad_id)::int as fuel,
                (select value from blocket_{3}.ad_params where "name" = 'gearbox' and ad_id = ads.ad_id)::int as gearbox
            from
                blocket_{3}.ads
            where
                modified_at::date between '{0}' and '{1}'
                and category in (2020,2040,2060)
            """.format(self.params.get_date_from(),
                       self.params.get_date_to(),
                       self.params.get_current_year(),
                       self.params.get_last_year())
        return query


class AdParamsInmoQuery:

    def dwh_ad_params(self) -> str:
        return "select * from ods.ads_inmo_params"

    def blocket_ad_params(self) -> str:
        """
        Method return str with query
        """
        query = """
            select
                ad_id::int ad_id_nk,
                (select value from ad_params where "name" = 'bathrooms' and ad_id = ads.ad_id)::int as bathrooms,
                (select value from ad_params where "name" = 'rooms' and ad_id = ads.ad_id)::int as rooms,
                (select value from ad_params where "name" = 'size' and ad_id = ads.ad_id)::int as meters,
                (select value from ad_params where "name" = 'estate_type' and ad_id = ads.ad_id)::int as estate_type,
                (select value from ad_params where "name" = 'new_realestate' and ad_id = ads.ad_id)::int as new_realestate,
                (select value from ad_params where "name" = 'services' and ad_id = ads.ad_id) as services,
                (select value from ad_params where "name" = 'currency' and ad_id = ads.ad_id)::varchar(10) as currency,
                (select value from ad_params where "name" = 'prev_currency' and ad_id = ads.ad_id)::varchar(10) as prev_currency
            from
                ads
            where
                modified_at::date between '{0}' and '{1}'
                and category in (1020,1040,1060,1080,1100,1120,1220,1240,1260)
            union all select
                ad_id::int ad_id_nk,
                (select value from blocket_{2}.ad_params where "name" = 'bathrooms' and ad_id = ads.ad_id)::int as bathrooms,
                (select value from blocket_{2}.ad_params where "name" = 'rooms' and ad_id = ads.ad_id)::int as rooms,
                (select value from blocket_{2}.ad_params where "name" = 'size' and ad_id = ads.ad_id)::int as meters,
                (select value from blocket_{2}.ad_params where "name" = 'estate_type' and ad_id = ads.ad_id)::int as estate_type,
                (select value from blocket_{2}.ad_params where "name" = 'new_realestate' and ad_id = ads.ad_id)::int as new_realestate,
                (select value from blocket_{2}.ad_params where "name" = 'services' and ad_id = ads.ad_id) as services,
                (select value from blocket_{2}.ad_params where "name" = 'currency' and ad_id = ads.ad_id)::varchar(10) as currency,
                (select value from blocket_{2}.ad_params where "name" = 'prev_currency' and ad_id = ads.ad_id)::varchar(10) as prev_currency
            from
                blocket_{2}.ads
            where
                modified_at::date between '{0}' and '{1}'
                and category in (1020,1040,1060,1080,1100,1120,1220,1240,1260)
            union all select
                ad_id::int ad_id_nk,
                (select value from blocket_{3}.ad_params where "name" = 'bathrooms' and ad_id = ads.ad_id)::int as bathrooms,
                (select value from blocket_{3}.ad_params where "name" = 'rooms' and ad_id = ads.ad_id)::int as rooms,
                (select value from blocket_{3}.ad_params where "name" = 'size' and ad_id = ads.ad_id)::int as meters,
                (select value from blocket_{3}.ad_params where "name" = 'estate_type' and ad_id = ads.ad_id)::int as estate_type,
                (select value from blocket_{3}.ad_params where "name" = 'new_realestate' and ad_id = ads.ad_id)::int as new_realestate,
                (select value from blocket_{3}.ad_params where "name" = 'services' and ad_id = ads.ad_id) as services,
                (select value from blocket_{3}.ad_params where "name" = 'currency' and ad_id = ads.ad_id)::varchar(10) as currency,
                (select value from blocket_{3}.ad_params where "name" = 'prev_currency' and ad_id = ads.ad_id)::varchar(10) as prev_currency
            from
                blocket_{3}.ads
            where
                modified_at::date between '{0}' and '{1}'
                and category in (1020,1040,1060,1080,1100,1120,1220,1240,1260)
            """.format(self.params.get_date_from(),
                       self.params.get_date_to(),
                       self.params.get_current_year(),
                       self.params.get_last_year())
        return query


class AdParamsBigSellerQuery:

    def dwh_ad_params(self) -> str:
        return "select * from stg.big_sellers_detail"

    def blocket_ad_params(self) -> str:
        """
        Method return str with query
        """
        query = """
            -- LINK TYPE
            select
                t.ad_id as ad_id_nk,
                t.list_id,
                fecha::date as list_time,
                max(link_type) as link_type
            from	
            (--t
            select
                a.ad_id,
                a.list_id,
                cast(list_time as date) as fecha,	
                b.value as link_type
            from
                public.ads a
            inner join
                public.ad_params b on a.ad_id = b.ad_id and b.name = 'link_type'

            group by
                1,2,3,4
            --
            union all
            --
            select
                a.ad_id,
                a.list_id,
                cast(list_time as date) as fecha,	
                b.value as link_type
            from
                blocket_{0}.ads a
            inner join
                blocket_{0}.ad_params b on a.ad_id = b.ad_id and b.name = 'link_type'

            group by
                1,2,3,4
            )T	
            group by 
                1,2,3
            """.format(self.params.get_current_year())
        return query
