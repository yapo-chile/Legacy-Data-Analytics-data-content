# pylint: disable=no-member
# utf-8


class TableauKpiFraudeQuery:
    def truncate_table(self) -> str:
        return "truncate dm_analysis.temp_tableau_kpi_fraud"

    def select_kpi_fraude(self) -> str:
        """
        Method return str with query
        """
        query = """
            select
                rp.*,
                case
                    when action_type = 'new' then 'not published'
                    when action_type is null and rp.grupo_revision = 'Automatico' then 'autopublished'
                    when action_type is null and rp.grupo_revision = 'Manual' then 'published'
                    when rp.action_type = 'editrefused' and a.approval_date is null then 'not published'
                    when rp.action_type = 'editrefused' and a.approval_date is not null then 'published'		
                    else 'published'
                end as "fraud_type",
                case
                    when rp.action_type = 'editrefused' then true::boolean 
                    else false::boolean
                end as "editrefused",
                a.creation_date,
                a.approval_date,
                ca.category_name,
                cm.category_main_name
            from 
                (select
                    case
                        when ad_id_main is null then sf.ad_id_nk
                        else ad_id_main
                    end as "main_ad_id",
                    fr.action_type,
                    case
                        when fr.admin_name is not null then 'Manual'
                        else 'Automatico'
                    end as "grupo_revision"
                from 
                (select
                        *
                    from 
                        (select 
                            ad_id as "ad_id_main",
                            'https://www2.yapo.cl/controlpanel?m=adqueue&queue=all&lock=0&a=show_ad&ad_id='|| ad_id::text || '&action_id=' || (max ((substring(link_cp, position('action_id=' in link_cp) + 10, position('#' in link_cp) - (position('action_id=' in link_cp) + 10) ))::int ))::text || '#' as "max_actionid_url"
                        from
                            stg.review_params
                        where review_date::date between '2019-01-01' and current_timestamp
                        group by 1) "max"
                left join
                        stg.review_params rp on rp.link_cp = "max".max_actionid_url
                where
                        refusal_reason_text = 'Fraude') fr
                full outer join
                    ods.scarface_fraud_deleted sf on sf.ad_id_nk = fr.ad_id_main) rp
            inner join
                (select
                    ad_id_nk,
                    approval_date,
                    creation_date,
                    a.category_id_fk
                from
                    ods.ad a
                where
                    creation_date::date between '2019-01-01' and current_timestamp) a on a.ad_id_nk = rp.main_ad_id
            left join
                ods.category ca on ca.category_id_pk = a.category_id_fk
            left join
                ods.category_main cm on cm.category_main_id_pk = ca.category_main_id_fk;
            """
        return query

