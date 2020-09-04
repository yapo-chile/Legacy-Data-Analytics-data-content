# pylint: disable=no-member
# utf-8


class RevParamsQuery:

    def clean_rev_params(self) -> str:
        return """delete from stg.review_params 
                    where review_date::date between '{}'::date 
                    and '{}'::date""".format(self.params.get_date_from(),
                       self.params.get_date_to())

    def blocket_rev_params(self) -> str:
        """
        Method return str with query
        """
        query = """
            select
            bb.ad_id,
            bb.subject,
            bb.region_name,
            bb.category_main_name,
            bb.category_name,
            bb.review_date,
            bb.review_time,
            bb.queue,
            bb.admin_name,
            bb."action",
            bb.refusal_reason_text,
            'https://www2.yapo.cl/controlpanel?m=adqueue&queue=all&lock=0&a=show_ad&ad_id='||bb.ad_id||'&action_id='||bb.action_id||'#' as link_cp,
            bb.action_type,
            bb.implio_decision
        from
            (select
                aa.ad_id,
                min(aa.action_id) as action_id,
                aa.subject,
                case
                    when aa.region = 1 then 'XV Arica & Parinacota' 
                    when aa.region = 2 then 'I Tarapacá'
                    when aa.region = 3 then 'II Antofagasta'
                    when aa.region = 4 then 'III Atacama'
                    when aa.region = 5 then 'IV Coquimbo'
                    when aa.region = 6 then 'V Valparaiso'
                    when aa.region = 7 then 'VI OHiggins'
                    when aa.region = 8 then 'VII Maule'
                    when aa.region = 9 then 'VIII Biobío'
                    when aa.region = 10 then 'IX Araucanía'
                    when aa.region = 11 then 'XIV Los Ríos'
                    when aa.region = 12 then 'X Los Lagos'
                    when aa.region = 13 then 'XI Aisén'
                    when aa.region = 14 then 'XII Magallanes & Antártica'
                    when aa.region = 15 then 'Región Metropolitana'
                    when aa.region = 16 then 'XVI Ñuble'
                    else 'Nulo'
                end as region_name,
                case
                    when aa.category in (1020, 1040, 1060, 1080, 1100, 1120, 1240, 1260,1220) then 'Inmuebles'
                    when aa.category in (2020, 2040, 2060, 2080, 2100, 2120) then 'Vehículos'
                    when aa.category in (5100, 5120, 5140, 5040, 5060, 5020, 5160) then 'Hogar'
                    when aa.category in (6020, 6060, 6080, 6100, 6120, 6140, 6160, 6180, 6200) then 'Tiempo Libre'
                    when aa.category in (3020, 3040, 3060, 3080) then 'Computadores & electrónica'
                    when aa.category in (7020, 7040, 7060, 7080) then 'Servicios, negocios y empleo'
                    when aa.category in (4040, 4020, 4060, 4080) then 'Moda, belleza y salud'
                    when aa.category in (9020, 9040, 9060) then 'Futura mamá, bebés y niños'
                    when aa.category = 8020 then 'Otros'
                    else 'Unknown'
                end as category_main_name,	
                case
                    when aa.category = 1220 then 'Comprar'
                    when aa.category = 1020 then 'Departamentos y piezas'
                    when aa.category = 1040 then 'Casas'
                    when aa.category = 1060 then 'Oficinas'
                    when aa.category = 1080 then 'Comercial e industrial'
                    when aa.category = 1100 then 'Terrenos'
                    when aa.category = 1120 then 'Estacionamientos, bodegas y otros'
                    when aa.category = 2020 then 'Autos, camionetas y 4x4'
                    when aa.category = 2040 then 'Camiones y furgones'
                    when aa.category = 2060 then 'Motos'
                    when aa.category = 2080 then 'Barcos, lanchas y aviones'
                    when aa.category = 2100 then 'Accesorios y piezas para vehículos'
                    when aa.category = 2120 then 'Otros vehículos'
                    when aa.category = 5100 then 'Moda y vestuario'
                    when aa.category = 5120 then 'Embarazadas, bebés y niños'
                    when aa.category = 5140 then 'Bolsos, bisutería y accesorios'
                    when aa.category = 6020 then 'Deportes, gimnasia y accesorios'
                    when aa.category = 6060 then 'Bicicletas, ciclismo y accesorios'
                    when aa.category = 6080 then 'Instrumentos musicales y accesorios'
                    when aa.category = 6100 then 'Música y películas (DVDs, etc.)'
                    when aa.category = 6120 then 'Libros y revistas'
                    when aa.category = 6140 then 'Animales y sus accesorios'
                    when aa.category = 6160 then 'Arte, antigüedades y colecciones'
                    when aa.category = 6180 then 'Hobbies y outdoor'
                    when aa.category = 6200 then 'Salud y belleza'
                    when aa.category = 3020 then 'Consolas, videojuegos y accesorios'
                    when aa.category = 3040 then 'Computadores y accesorios'
                    when aa.category = 3060 then 'Celulares, teléfonos y accesorios'
                    when aa.category = 3080 then 'Audio, TV, video y fotografía'
                    when aa.category = 7020 then 'Ofertas de empleo'
                    when aa.category = 7040 then 'Busco empleo'
                    when aa.category = 7060 then 'Servicios'
                    when aa.category = 7080 then 'Negocios, maquinaria y construcción'
                    when aa.category = 8020 then 'Otros productos'
                    when aa.category = 4020 then 'Moda, vestuario y calzado'
                    when aa.category = 4040 then 'Bolsos, bisutería y accesorios'
                    when aa.category = 4060 then 'Salud y belleza'
                    when aa.category = 5020 then 'Muebles'
                    when aa.category = 5020 then 'Muebles y artículos del hogar'
                    when aa.category = 5040 then 'Electrodomésticos'
                    when aa.category = 5040 then 'Electrodomésticos'
                    when aa.category = 5060 then 'Jardín y herramientas'
                    when aa.category = 5060 then 'Jardín y herramientas'
                    when aa.category = 5160 then 'Otros artículos del hogar'
                    when aa.category = 9020 then 'Vestuario futura mamá y niños'
                    when aa.category = 9040 then 'Juguetes'
                    when aa.category = 9060 then 'Coches y artículos infantiles'
                    when aa.category = 1240 then 'Arrendar'
                    when aa.category = 1260 then 'Arriendo de temporada'
                    when aa.category = 4080 then 'Calzado'
                    else 'Unknown'
                end as category_name,
                aa.review_time::date as review_date,
                to_char(aa.review_time, 'HH24:MI:SS') as review_time,
                aa.queue,
                adm.fullname as admin_name,
                aa.transition as "action",
                rl.refusal_reason_text,
                --'https://www2.yapo.cl/controlpanel?m=adqueue&queue=all&lock=0&a=show_ad&ad_id='||aa.ad_id||'&action_id='||aa.action_id||'#' as link_cp
                aa.action_type,
                aa.implio_decision
            from
                (select
                    ada.ad_id,
                    ada.action_id,
                    ads.subject,
                    ads.region,
                    ads.category,
                    acs."timestamp" as review_time, --to_char(acs."timestamp", 'YYYY-MM-DD HH24:MI:SS')::timestamp as review_time,
                    ada.queue,
                    acs.transition,
                    ada.action_type,
                    apa.value as implio_decision
                from
                    public.ad_actions ada
                inner join
                    public.action_states acs using(ad_id,action_id)
                left join
                    public.ads using(ad_id)
                left join
                    public.action_params apa
                    on ada.ad_id = apa.ad_id and ada.action_id = apa.action_id and apa."name" = 'ad_evaluation_result'
                where
                    acs.state in ('accepted','refused')
                    and acs.transition in ('accept','accept_w_chngs','refuse')
                    and acs."timestamp"::date between '{DATE_FROM}'::date and '{DATE_TO}'::date
                union all select
                    ada.ad_id,
                    ada.action_id,
                    ads.subject,
                    ads.region,
                    ads.category,
                    acs."timestamp" as review_time, --to_char(acs."timestamp", 'YYYY-MM-DD HH24:MI:SS')::timestamp as review_time,
                    ada.queue,
                    acs.transition,
                    ada.action_type,
                    apa.value as implio_decision
                from
                    blocket_{CURRENT_YEAR}.ad_actions ada
                inner join
                    blocket_{CURRENT_YEAR}.action_states acs using(ad_id,action_id)
                left join
                    blocket_{CURRENT_YEAR}.ads using(ad_id)
                left join
                    blocket_{CURRENT_YEAR}.action_params apa
                    on ada.ad_id = apa.ad_id and ada.action_id = apa.action_id and apa."name" = 'ad_evaluation_result'
                where
                    acs.state in ('accepted','refused')
                    and acs.transition in ('accept','accept_w_chngs','refuse')
                    and acs."timestamp"::date between '{DATE_FROM}'::date and '{DATE_TO}'::date
                union all select
                    ada.ad_id,
                    ada.action_id,
                    ads.subject,
                    ads.region,
                    ads.category,
                    acs."timestamp" as review_time, --to_char(acs."timestamp", 'YYYY-MM-DD HH24:MI:SS')::timestamp as review_time,
                    ada.queue,
                    acs.transition,
                    ada.action_type,
                    apa.value as implio_decision
                from
                    blocket_{LAST_YEAR}.ad_actions ada
                inner join
                    blocket_{LAST_YEAR}.action_states acs using(ad_id,action_id)
                left join
                    blocket_{LAST_YEAR}.ads using(ad_id)
                left join
                    blocket_{LAST_YEAR}.action_params apa
                    on ada.ad_id = apa.ad_id and ada.action_id = apa.action_id and apa."name" = 'ad_evaluation_result'
                where
                    acs.state in ('accepted','refused')
                    and acs.transition in ('accept','accept_w_chngs','refuse')
                    and acs."timestamp"::date between '{DATE_FROM}'::date and '{DATE_TO}'::date
                )aa
            left join
                review_log rl using(ad_id,review_time)
            left join
                admins adm using(admin_id)
            group by
                1,3,4,5,6,7,8,9,10,11,12,13,14
            )bb
            """.format(DATE_FROM=self.params.get_date_from(),
                       DATE_TO=self.params.get_date_to(),
                       CURRENT_YEAR=self.params.get_current_year(),
                       LAST_YEAR=self.params.get_last_year())
        return query
