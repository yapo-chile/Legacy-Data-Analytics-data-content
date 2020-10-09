# pylint: disable=no-member
# utf-8

class Query:
    """
    Class that store all querys
    """

    def get_blocket_ads_created_daily(self) -> str:
        """
        Method return str with query
        """
        queryBlocket = """
                    select
                        z.ad_id,
                        z.list_id,
                        z.user_id,
                        z.account_id,
                        z.email,
                        z.platform_id_nk,
                        z.creation_date,
                        z.approval_date,
                        z.deletion_date,
                        z.category,
                        z.region,
                        z.type,
                        z.company_ad,
                        z.price,
                        z.reason_removed_id_nk,
                        z.reason_removed_detail_id_nk,
                        z.action_type,
                        z.communes_id_nk,
                        z.phone,
                        z.body,
                        z.subject,
                        z.user_name
                    from (
                        select 
                        distinct aa.ad_id, 
                        a.list_id,
                        a.user_id, 
                        ac.account_id,
                        u.email,
                        ap."value" as platform_id_nk,
                        case
                            when (acts.action_id = 1 and acts.state = 'reg' and aa.action_type in ('edit', 'new', 'import')) then acts.timestamp 
                            else null
                        end creation_date,
                        case
                            when (acts.state = 'accepted' and aa.action_type = 'new') then  acts.timestamp
                            else null
                        end approval_date,
                        case
                            when (acts.state in('deleted', 'refused') and aa.action_type in ('delete', 'post_refusal')) then  acts.timestamp
                            else null
                        end deletion_date,
                        a.category, 
                        a.region,
                        a.type,
                        a.company_ad,
                        a.price,
                        case
                            when (acts.state in('deleted', 'refused') and aa.action_type in ('delete', 'post_refusal')) then acts.transition 
                            else null
                        end reason_removed_id_nk,
                        case
                            when (acts.state in('deleted', 'refused') and aa.action_type in ('delete', 'post_refusal')) then rp."value"
                            else null
                        end reason_removed_detail_id_nk,
                        aa.action_type,
                        dp."value"::int as communes_id_nk,
                        a.phone, 
                        a.body,
                        a.subject,
                        a.name	as user_name
                        from 
                            public.action_states as acts
                        left join
                            public.ad_actions as aa on (aa.ad_id = acts.ad_id and aa.action_id = acts.action_id) 
                        left join 
                            public.ads as a on aa.ad_id = a.ad_id
                        left join 
                            public.action_params as ap on (ap.ad_id = aa.ad_id and ap.action_id = aa.action_id and ap.name = 'source')
                        left join 
                            public.users as u on u.user_id = a.user_id
                        left join 
                            public.accounts as ac on ac.user_id = u.user_id
                        left join
                            public.ad_params as dp on (dp.ad_id = a.ad_id and dp.name = 'communes')
                        left join 
                            public.action_params as rp on (rp.ad_id = aa.ad_id and rp.action_id = aa.action_id and rp.name = 'deletion_reason') 
                        where 
                            acts.state in ('reg', 'accepted', 'deleted', 'refused')
                            and acts.timestamp between '{0} 00:00:00' and '{1} 23:59:59'
                        union all
                        select 
                            distinct aa.ad_id, 
                            a.list_id,
                            a.user_id, 
                            ac.account_id,
                            u.email,
                            ap."value" as platform_id_nk,
                            case
                                when (acts.action_id = 1 and acts.state = 'reg' and aa.action_type in ('edit', 'new', 'import')) then acts.timestamp 
                                else null
                            end creation_date,
                            case
                                when  (acts.state = 'accepted' and aa.action_type = 'new') then  acts.timestamp
                                else null
                            end approval_date,
                            case
                                when (acts.state in('deleted', 'refused') and aa.action_type in ('delete', 'post_refusal')) then  acts.timestamp
                                else null
                            end deletion_date,
                            a.category, 
                            a.region,
                            a.type,
                            a.company_ad,
                            a.price,
                            case
                                when (acts.state in('deleted', 'refused') and aa.action_type in ('delete', 'post_refusal')) then acts.transition 
                                else null
                            end reason_removed_id_nk,
                            case
                                when (acts.state in('deleted', 'refused') and aa.action_type in ('delete', 'post_refusal')) then rp."value"
                                else null
                            end reason_removed_detail_id_nk,
                            aa.action_type,
                            dp."value"::int as communes_id_nk,
                            a.phone, 
                            a.body,
                            a.subject,
                            a.name as user_name	
                        from 
                            blocket_{2}.action_states as acts
                        left join
                            blocket_{2}.ad_actions as aa on (aa.ad_id = acts.ad_id and aa.action_id = acts.action_id) 
                        left join 
                            blocket_{2}.ads as a on aa.ad_id = a.ad_id
                        left join 
                            blocket_{2}.action_params as ap on (ap.ad_id = aa.ad_id and ap.action_id = aa.action_id and ap.name = 'source')
                        left join 
                            public.users as u on u.user_id = a.user_id
                        left join 
                            public.accounts as ac on ac.user_id = u.user_id
                        left join
                            blocket_{2}.ad_params as dp on (dp.ad_id = a.ad_id and dp.name = 'communes')
                        left join 
                            blocket_{2}.action_params as rp on (rp.ad_id = aa.ad_id and rp.action_id = aa.action_id and rp.name = 'deletion_reason')
                        where 
                            acts.state in ('reg', 'accepted', 'deleted', 'refused')
                            and acts.timestamp between '{0} 00:00:00' and '{1} 23:59:59'
                        union all
                        select 
                            distinct aa.ad_id, 
                            a.list_id,
                            a.user_id, 
                            ac.account_id,
                            u.email,
                            ap."value" as platform_id_nk,
                            case
                                when (acts.action_id = 1 and acts.state = 'reg' and aa.action_type in ('edit', 'new', 'import')) then acts.timestamp 
                                else null
                            end creation_date,
                            case
                                when  (acts.state = 'accepted' and aa.action_type = 'new') then  acts.timestamp
                                else null
                            end approval_date,
                            case
                                when (acts.state in('deleted', 'refused') and aa.action_type in ('delete', 'post_refusal')) then  acts.timestamp
                                else null
                            end deletion_date,
                            a.category, 
                            a.region,
                            a.type,
                            a.company_ad,
                            a.price,
                            case
                                when (acts.state in('deleted', 'refused') and aa.action_type in ('delete', 'post_refusal')) then acts.transition 
                                else null
                            end reason_removed_id_nk,
                            case
                                when (acts.state in('deleted', 'refused') and aa.action_type in ('delete', 'post_refusal')) then rp."value"
                                else null
                            end reason_removed_detail_id_nk,
                            aa.action_type,
                            dp."value"::int as communes_id_nk,
                            a.phone, 
                            a.body,
                            a.subject,
                            a.name as user_name	
                        from 
                            blocket_{3}.action_states as acts
                        left join
                            blocket_{3}.ad_actions as aa on (aa.ad_id = acts.ad_id and aa.action_id = acts.action_id) 
                        left join 
                            blocket_{3}.ads as a on aa.ad_id = a.ad_id
                        left join 
                            blocket_{3}.action_params as ap on (ap.ad_id = aa.ad_id and ap.action_id = aa.action_id and ap.name = 'source')
                        left join 
                            public.users as u on u.user_id = a.user_id
                        left join 
                            public.accounts as ac on ac.user_id = u.user_id
                        left join
                            blocket_{3}.ad_params as dp on (dp.ad_id = a.ad_id and dp.name = 'communes')
                        left join 
                            blocket_{3}.action_params as rp on (rp.ad_id = aa.ad_id and rp.action_id = aa.action_id and rp.name = 'deletion_reason')
                        where 
                            acts.state in ('reg', 'accepted', 'deleted', 'refused')
                            and acts.timestamp between '{0} 00:00:00' and '{1} 23:59:59'
                        ) z
                    where 
                    z.action_type in ('edit', 'new', 'import','delete', 'post_refusal')
                    and (z.creation_date is not null or z.approval_date is not null or z.deletion_date is not null)
            """.format(self.params.get_date_from(),
                       self.params.get_date_to(),
                       self.params.get_current_year(),
                       self.params.get_last_year())
        return queryBlocket

    def get_stg_ads_created_daily(self) -> str:
        """
        Method return str with query
        """
        queryDwh = """
                select 
                    a1.ad_id_nk,
                    a1.seller_id_fk,
                    a1.platform_id_fk,
                    a1.creation_date,
                    a1.category_id_fk,
                    a1.region_id_fk,
                    a1.ad_type_id_fk,
                    a1.pri_pro_id_fk,
                    a1.action_type,
                    a1.price,
                    a1.insert_date,
                    a1.update_date,
                    a1.communes_id_nk,
                    a1.phone,
                    a1.body,
                    a1.subject,
                    a1.user_name
                from (
                    select  
                        sad.ad_id as ad_id_nk
                        ,sel.seller_id_pk as seller_id_fk
                        ,coalesce(pla.platform_id_pk,0) as platform_id_fk
                        ,sad.creation_date
                        ,coalesce(cat.category_id_pk,0) as category_id_fk
                        ,coalesce(reg.region_id_pk,0) as region_id_fk
                        ,coalesce(adt.ad_type_id_pk,0) as ad_type_id_fk
                        ,coalesce(pio.pri_pro_id_pk,0) as pri_pro_id_fk
                        ,sad.action_type
                        ,sad.price
                        ,now() as insert_date
                        ,now() as update_date
                        ,sad.communes_id_nk
                        ,sad.phone
                        ,sad.body
                        ,sad.subject
                        ,sad.user_name	
                    from dm_analysis.temp_stg_ad sad
                        left join ods.ad_type adt on (adt.ad_type_id_nk = sad."type" 
                                                      and sad.creation_date >= adt.date_from 
                                                      and sad.creation_date <= adt.date_to)
                        left join ods.category cat on (cat.category_id_nk = sad.category 
                                                       and sad.creation_date >= cat.date_from 
                                                       and sad.creation_date <= cat.date_to)
                        left join ods.platform pla on (pla.platform_id_nk = sad.platform_id_nk 
                                                       and sad.creation_date >= pla.date_from 
                                                       and sad.creation_date <= pla.date_to)
                        left join ods.pri_pro pio on (pio.pri_pro_id_nk = sad.company_ad 
                                                      and sad.creation_date >= pio.date_from 
                                                      and sad.creation_date <= pio.date_to)
                        left join ods.region reg on (reg.region_id_nk = sad.region 
                                                     and sad.creation_date >= reg.date_from 
                                                     and sad.creation_date <= reg.date_to)
                        left join ods.seller sel on (sel.seller_id_nk = sad.email )
                    where
                        sad.creation_date between '{0} 00:00:00' and '{1} 23:59:59'
                        order by sad.ad_id, sad.creation_date) a1
                where not exists (
                    select 1 from ods.ad a2 where a2.ad_id_nk = a1.ad_id_nk)    
            """.format(self.params.get_date_from(),
                       self.params.get_date_to())
        return queryDwh

    def upd_approval_date_to_ods_ad_table(self) -> str:
        """
        Method that returns events of the day
        """
        command = """
                    UPDATE dm_analysis.temp_ods_ad oad
                    SET approval_date = saa.approval_date
                        ,list_id_nk = saa.list_id_nk
                        ,price = (case when saa.price = -1 then null else saa.price end)
                        ,update_date = now()
                    FROM (
                            select 
                                a1.ad_id as ad_id_nk
                                ,a1.approval_date
                                ,coalesce(a1.price,-1) as price
                                ,a1.list_id as list_id_nk
                            from (
                                    select  
                                        sad.ad_id
                                        ,sad.user_id
                                        ,sad.account_id
                                        ,sad.email
                                        ,sad.approval_date
                                        ,sad.category
                                        ,sad.region
                                        ,sad."type"
                                        ,sad.company_ad
                                        ,sad.price
                                        ,sad.list_id
                                        ,sad.action_type
                                        ,rank() over(partition by sad.ad_id order by sad.approval_date)	
                                    from dm_analysis.temp_stg_ad sad
                                    where
                                        sad.approval_date between '{0} 00:00:00' and '{1} 23:59:59') a1
                                        inner join dm_analysis.temp_ods_ad a2 on (a2.ad_id_nk = a1.ad_id)
                            where 
                                rank=1
                                and a2.approval_date is null
                            order by a1.approval_date) saa
                    WHERE oad.ad_id_nk = saa.ad_id_nk
                """.format(self.params.get_date_from(),
                           self.params.get_date_to())
        return command

    def upd_left_approval_date_to_ods_ad_table(self) -> str:
        """
        Method that returns events of the day
        """
        command = """
                    UPDATE dm_analysis.temp_ods_ad
                    SET approval_date = creation_date
                    WHERE approval_date is null
                        and action_type = 'import'
                        and creation_date::date = 
                        '""" + self.params.get_date_from() + """'::date
                        and creation_date::date <= 
                        '""" + self.params.get_date_to() + """'::date """
        return command

    def upd_deletion_date_to_ods_ad_table(self) -> str:
        """
        Method that returns events of the day
        """
        command = """
                    UPDATE dm_analysis.temp_ods_ad oad
                    SET deletion_date = sdd.deletion_date
                        ,reason_removed_id_fk = sdd.reason_removed_id_fk
                        ,reason_removed_detail_id_fk = sdd.reason_removed_detail_id_fk
                        ,update_date = now()
                    FROM (
                            select 
                                 a1.ad_id as ad_id_nk
                                ,a1.deletion_date
                                ,a1.reason_removed_id_fk
                                ,a1.reason_removed_detail_id_fk
                            from (
                                    select  
                                        sad.ad_id 
                                        ,sad.user_id
                                        ,sad.account_id
                                        ,sad.email
                                        ,sad.deletion_date
                                        ,sad.category
                                        ,sad.region
                                        ,sad."type"
                                        ,sad.company_ad
                                        ,sad.price
                                        ,coalesce(rer.reason_removed_id_pk,0) as reason_removed_id_fk
                                        ,coalesce(rrd.reason_removed_detail_id_pk,0) as reason_removed_detail_id_fk
                                        ,rank() over(partition by sad.ad_id order by sad.deletion_date, sad.ad_id)	
                                    from dm_analysis.temp_stg_ad sad
                                        left join ods.reason_removed rer on (rer.reason_removed_id_nk = sad.reason_removed_id_nk 
                                                                             and sad.deletion_date >= rer.date_from 
                                                                             and sad.deletion_date <= rer.date_to)
                                        left join ods.reason_removed_detail rrd on (rrd.reason_removed_detail_id_nk = sad.reason_removed_detail_id_nk::integer 
                                                                                    and sad.deletion_date >= rrd.date_from 
                                                                                    and sad.deletion_date <= rrd.date_to)
                                    where
                                        sad.deletion_date between '{0} 00:00:00' and '{1} 23:59:59') a1
                                        inner join ods.ad a2 on (a2.ad_id_nk = a1.ad_id)
                            where 
                                rank=1
                                and a2.deletion_date is null
                            order by a1.deletion_date) sdd
                    WHERE oad.ad_id_nk = sdd.ad_id_nk
                """.format(self.params.get_date_from(),
                           self.params.get_date_to())
        return command

    def upd_rank_approval_to_ods_ad_table(self) -> str:
        """
        Method that returns events of the day
        """
        command = """
                    UPDATE dm_analysis.temp_ods_ad oad
                    SET rank_approval = coalesce((select max(a1.rank_approval) + 1 from ods.ad a1 where a1.seller_id_fk = odr.seller_id_fk), 1)
                    FROM (
                            select 
                                 a2.ad_id_pk
                                ,a2.seller_id_fk
                            from ods.ad a2
                            where 
                                a2.rank_approval is null 
                                and a2.approval_date is not null
                            order by a2.approval_date, a2.ad_id_nk) odr
                    WHERE oad.ad_id_pk = odr.ad_id_pk
                """
        return command

    def upd_first_approval_to_ods_seller_table(self) -> str:
        """
        Method that returns events of the day
        """
        command = """
                    UPDATE dm_analysis.temp_ods_seller os1
                    SET first_approval_date = mad.min_approval_date,
                        update_date = now()
                    FROM (
                        select
                            os2.seller_id_pk,
                            min(oad.approval_date) as min_approval_date
                        from
                            ods.seller os2
                            inner join ods.ad oad on (oad.seller_id_fk = os2.seller_id_pk)
                        where
                            os2.first_approval_date is null
                        group by 1
                    ) mad
                    WHERE os1.seller_id_pk = mad.seller_id_pk
                """
        return command

    def delete_stg_ad_table(self) -> str:
        """
        Method that returns events of the day
        """
        command = """
                    truncate table dm_analysis.temp_stg_ad 
                """
        return command

    def delete_ods_ad_table(self) -> str:
        """
        Method that returns events of the  day
        """
        command = """
                    delete from dm_analysis.temp_ods_ad where 
                    creation_date::date >= 
                    '""" + self.params.get_date_from() + """'::date
                    and creation_date::date <= 
                    '""" + self.params.get_date_to() + """'::date """

        return command
