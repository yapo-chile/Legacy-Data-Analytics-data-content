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
