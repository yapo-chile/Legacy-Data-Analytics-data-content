# pylint: disable=no-member
# utf-8


class Query:
    """
    Class that store all querys
    """

    def get_blocket_sellers_created_daily(self) -> str:
        """
        Method return str with query
        """
        queryBlocket = """
                    select
                        z.user_id as seller_id_blocket_nk,
                        bool_or(case when z.account_id is null then false else true end) as has_account,
                        z.email as seller_id_nk,
                        z.email,
                        0::INTEGER as pri_pro_id_fk,
                        min(z.creation_date) as seller_creation_date
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
                    group by z.user_id, z.email
            """.format(self.params.get_date_from(),
                       self.params.get_date_to(),
                       self.params.get_current_year(),
                       self.params.get_last_year())
        return queryBlocket

    def get_blocket_users_account(self) -> str:
        """
        Method return str with query
        """
        query = """
                SELECT user_id as user_id_nk,  
	                   min(creation_date) as account_creation_data, 
                       bool_or(is_company) as pri_pro_id_nk, 
                       lower(email) as email_account
                FROM public.accounts
                WHERE creation_date between '{0} 00:00:00' and '{1} 23:59:59'
                      and status = 'active' or status='inactive'
                group by lower(email),user_id
            """.format(self.params.get_date_from(),
                       self.params.get_date_to())
        return query

    def get_blocket_account_params_is_pro(self) -> str:
        """
        Method return str with query
        """
        query = """
                SELECT 
                    z.user_id,
                    z.account_id,
                    z.category,
                    RANK () OVER (PARTITION by z.user_id, z.account_id ORDER BY z.category ) category_rank 
                FROM(	
                    select
                        x.user_id,
                        x.account_id,
                        unnest(STRING_TO_ARRAY(x.categories, ',')) as category
                    from (	
                        select 
                            a.user_id,
                            ap.account_id,
                            ap.value as categories
                        from
                            public.account_params ap
                        inner join
                            public.accounts a using(account_id)
                        where 
                            ap."name" = 'is_pro_for') x) z
            """
        return query

    def get_stg_sellers_created_daily(self) -> str:
        """
        Method return str with query
        """
        query = """
                SELECT
                    z.seller_id_blocket_nk,
                    z.has_account,
                    z.seller_id_nk,
                    z.email,
                    z.pri_pro_id_fk,
                    z.seller_creation_date,
                    now() as insert_date,
                    now() as update_date
                FROM
                    (select
                        scd.seller_id_blocket_nk,
                        scd.has_account,
                        scd.seller_id_nk,
                        coalesce(s.seller_id_pk, 0) as seller_id_pk_aux,
                        scd.email,
                        scd.pri_pro_id_fk,
                        scd.seller_creation_date
                    from dm_analysis.temp_stg_seller_created_daily scd
                        left join dm_analysis.temp_ods_seller s on (s.seller_id_nk = scd.seller_id_nk)) z
                WHERE
                    seller_id_pk_aux = 0
            """
        return query

    def get_stg_seller_pro(self) -> str:
        """
        Method return str with query
        """
        query = """
                SELECT
                    s.seller_id_pk as seller_id_fk,	
                    sp.user_id, 
                    sp.account_id as account_id_nk, 
                    sp.category_rank,
                    c.category_id_pk as category_id_fk,
                    cm.category_main_id_pk as category_main_id_fk
                FROM dm_analysis.temp_stg_seller_pro sp
                    inner join ods.seller s on (sp.user_id = s.seller_id_blocket_nk)
                    left join ods.category c on (sp.category::int = c.category_id_nk
                                                 and c.date_to::date = '2199-12-31'::date)
                    left join ods.category_main cm on (c.category_main_id_fk = cm.category_main_id_pk
                                                       and cm.date_to::date = '2199-12-31'::date)
            """
        return query

    def upd_ods_seller_table(self) -> str:
        """
        Method that returns events of the day
        """
        command = """
                    UPDATE dm_analysis.temp_ods_seller s
                    SET has_account = true,
                        pri_pro_id_fk = pp.pri_pro_id_pk,
                        update_date = now()
                    FROM dm_analysis.temp_stg_account a
                        left join ods.pri_pro pp on (pp.pri_pro_id_nk = a.pri_pro_id_nk 
                                                     and pp.date_to::date = '2199-12-31'::date)
                    WHERE s.seller_id_nk = a.email_account
                """
        return command

    def delete_stg_account_table(self) -> str:
        """
        Method that returns events of the day
        """
        command = """
                    truncate table dm_analysis.temp_stg_account 
                """
        return command

    def delete_stg_seller_created_daily_table(self) -> str:
        """
        Method that returns events of the day
        """
        command = """
                    truncate table dm_analysis.temp_stg_seller_created_daily 
                """
        return command

    def delete_stg_seller_pro_table(self) -> str:
        """
        Method that returns events of the day
        """
        command = """
                    truncate table dm_analysis.temp_stg_seller_pro 
                """
        return command

    def delete_ods_seller_pro_details_table(self) -> str:
        """
        Method that returns events of the day
        """
        command = """
                    truncate table dm_analysis.temp_ods_seller_pro_details 
                """
        return command

    def delete_ods_seller_table(self) -> str:
        """
        Method that returns events of the day
        """
        command = """
                    delete from dm_analysis.temp_ods_seller where 
                    seller_creation_date::date >= 
                    '""" + self.params.get_date_from() + """'::date
                    and seller_creation_date::date <= 
                    '""" + self.params.get_date_to() + """'::date """

        return command
