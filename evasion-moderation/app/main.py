# pylint: disable=no-member
# utf-8
import sys
import logging
from infraestructure.conf import configFile
from infraestructure.psql import database
from infraestructure.stringIteratorIO import StringIteratorIO
from infraestructure.stringIteratorIO import cleanCsvValue
from interfaces.readParams import readParams
from interfaces.timeExecution import timeExecution


if __name__ == '__main__':
    te = timeExecution()
    logger = logging.getLogger('content-evasion-moderation')
    dateformat = """%(asctime)s,%(msecs)d %(levelname)-2s """
    infoFormat = """[%(filename)s:%(lineno)d] %(message)s"""
    format = dateformat + infoFormat
    logging.basicConfig(format=format, level=logging.INFO)
    params = readParams(sys.argv)
    conf = configFile(params.getConfigurationFile())
    rdbmsEndpoint = database(conf.getVal('ENDPOINTDB.HOST'),
                             conf.getVal('ENDPOINTDB.PORT'),
                             conf.getVal('ENDPOINTDB.DATABASE'),
                             conf.getVal('ENDPOINTDB.USERNAME'),
                             conf.getVal('ENDPOINTDB.PASSWORD'))
    rdbmsSource = database(conf.getVal('SOURCEDB.HOST'),
                           conf.getVal('SOURCEDB.PORT'),
                           conf.getVal('SOURCEDB.DATABASE'),
                           conf.getVal('SOURCEDB.USERNAME'),
                           conf.getVal('SOURCEDB.PASSWORD'))
    deleteEvasion = """ delete from """ + conf.getVal('ENDPOINTDB.TABLE.ME') + """
                   where review_time between '""" + params.getDateFrom() + """'
                   and '""" + params.getDateTo() + """' """
    deleteEvasionDetails = """ delete from """ + conf.getVal('ENDPOINTDB.TABLE.MED') + """
                   where review_time::date 
                   between '""" + params.getDateFrom() + """'
                   and '""" + params.getDateTo() + """' """

    rdbmsEndpoint.executeCommand(deleteEvasion)
    rdbmsEndpoint.executeCommand(deleteEvasionDetails)
    queryEvasionModeration = """
    select
          rank() over(partition by u.email
            order by rl.review_time::date)
          as review_order,
          rank() over(partition by min(p.date_start)
            order by rl.review_time::date)
          as pack_order,
          rank() over(partition by min(lf.purchase_date)
            order by rl.review_time::date)
          as ifee_order,
          u.email,
          rl.review_time::date,
          min(p.date_start) as pack_start_date,
          min(lf.purchase_date) as ifee_purchase_date
          from
            review_log rl
          left join
          (
            select
              ad_id,
              user_id
              from
              ads
              union all select
              ad_id,
              user_id
              from
                blocket_""" + params.getCurrentYear() + """.ads
                union all select
                ad_id,
                user_id
                from
                blocket_""" + params.getLastYear() + """.ads
              )aa
                using(ad_id)
              left join
                users u
                using(user_id)
              left join
                accounts acc
                using(user_id)
              left join
              (
                select
                  account_id,
                  date_start,
                  date_end
                  from
                    packs
              )p
              on acc.account_id = p.account_id
                and rl.review_time < p.date_start
              left join
              (
                select
                  pd.ad_id,
                  min(pu.receipt) as purchase_date
                  from
                    purchase pu
                  join
                    purchase_detail pd
                    using(purchase_id)
                  where
                    pd.product_id in (60,61,62)
                    and pu.status in ('paid','sent','confirmed')
                  group by 1
              )lf
              using(ad_id)
              where
                rl.refusal_reason_text
                in ('Profesional INMO','Profesional Vehículos')
                and rl.review_time::date
                between '"""+params.getDateFrom()+"""'::date
                and '"""+params.getDateTo()+"""'::date
              group by
              4,5 """

    dataEvasion = rdbmsSource.selectToDict(queryEvasionModeration)
    rdbmsEndpoint.copyEvasion(conf.getVal('ENDPOINTDB.TABLE.ME'),
                              dataEvasion)

    queryEvasionModerationDetails = """
  select
    u.email,
    rank() over(partition by u.email order by rl.review_time) as review_order,
    rl.ad_id,
    adm.fullname as admin_name,
    rl.review_time,
    rl.queue,
    rl.refusal_reason_text::varchar(200),
    p.account_id,
    p.pack_id,
    p."type"::varchar(20),
    p.slots,
    p.date_start,
    p.date_end,
    p.product_name::varchar(25),
    p.tipo_pack::varchar(15),
    lf.ad_id as ifee_ad_id,
    lf.product_name::varchar(20) as ifee_name,
    lf.purchase_date as ifee_purchase_date,
    lf.ifee_price::int
    from
      review_log rl
      left join
      (
        select
          ad_id,
          user_id
        from
          ads
        union all 
        select
          ad_id,
          user_id
        from
          blocket_""" + params.getCurrentYear() + """.ads
        union all
        select
          ad_id,
          user_id
        from
          blocket_""" + params.getLastYear() + """.ads
        )aa
        using(ad_id)
        left join
          users u using(user_id)
        left join
          accounts acc using(user_id)
        left join
          admins adm using(admin_id)
        left join
        (
          select
            account_id,
            pack_id,
            "type",
            slots,
            date_start,
            date_end,
            case when product_id = 10000 then 'Pack Autos Personalizado'
                when product_id = 11005 then 'Pack Autos 5 Monthly'
                when product_id = 11010 then 'Pack Autos 10 Monthly'
                when product_id = 11020 then 'Pack Autos 20 Monthly'
                when product_id = 11030 then 'Pack Autos 30 Monthly'
                when product_id = 11040 then 'Pack Autos 40 Monthly'
                when product_id = 11050 then 'Pack Autos 50 Monthly'
                when product_id = 11075 then 'Pack Autos 75 Monthly'
                when product_id = 11100 then 'Pack Autos 100 Monthly'
                when product_id = 11125 then 'Pack Autos 125 Monthly'
                when product_id = 12005 then 'Pack Autos 5 Quarterly'
                when product_id = 12010 then 'Pack Autos 10 Quarterly'
                when product_id = 12020 then 'Pack Autos 20 Quarterly'
                when product_id = 12030 then 'Pack Autos 30 Quarterly'
                when product_id = 12040 then 'Pack Autos 40 Quarterly'
                when product_id = 12050 then 'Pack Autos 50 Quarterly'
                when product_id = 12075 then 'Pack Autos 75 Quarterly'
                when product_id = 12100 then 'Pack Autos 100 Quarterly'
                when product_id = 12125 then 'Pack Autos 125 Quarterly'
                when product_id = 13005 then 'Pack Autos 5 Biannual'
                when product_id = 13010 then 'Pack Autos 10 Biannual'
                when product_id = 13020 then 'Pack Autos 20 Biannual'
                when product_id = 13030 then 'Pack Autos 30 Biannual'
                when product_id = 13040 then 'Pack Autos 40 Biannual'
                when product_id = 13050 then 'Pack Autos 50 Biannual'
                when product_id = 13075 then 'Pack Autos 75 Biannual'
                when product_id = 13100 then 'Pack Autos 100 Biannual'
                when product_id = 13125 then 'Pack Autos 125 Biannual'
                when product_id = 14005 then 'Pack Autos 5 Annual'
                when product_id = 14010 then 'Pack Autos 10 Annual'
                when product_id = 14020 then 'Pack Autos 20 Annual'
                when product_id = 14030 then 'Pack Autos 30 Annual'
                when product_id = 14040 then 'Pack Autos 40 Annual'
                when product_id = 14050 then 'Pack Autos 50 Annual'
                when product_id = 14075 then 'Pack Autos 75 Annual'
                when product_id = 14100 then 'Pack Autos 100 Annual'
                when product_id = 14125 then 'Pack Autos 125 Annual'
                when product_id = 20000 then 'Pack Inmo Personalizado'
                when product_id = 21005 then 'Pack Inmo 5 mensual'
                when product_id = 21010 then 'Pack Inmo 10 mensual'
                when product_id = 21020 then 'Pack Inmo 20 mensual'
                when product_id = 21030 then 'Pack Inmo 30 mensual'
                when product_id = 21040 then 'Pack Inmo 40 mensual'
                when product_id = 21050 then 'Pack Inmo 50 mensual'
                when product_id = 21075 then 'Pack Inmo 75 mensual'
                when product_id = 21100 then 'Pack Inmo 100 mensual'
                when product_id = 21125 then 'Pack Inmo 125 mensual'
                when product_id = 22005 then 'Pack Inmo 5 trimestral'
                when product_id = 22010 then 'Pack Inmo 10 trimestral'
                when product_id = 22020 then 'Pack Inmo 20 trimestral'
                when product_id = 22030 then 'Pack Inmo 30 trimestral'
                when product_id = 22040 then 'Pack Inmo 40 trimestral'
                when product_id = 22050 then 'Pack Inmo 50 trimestral'
                when product_id = 22075 then 'Pack Inmo 75 trimestral'
                when product_id = 22100 then 'Pack Inmo 100 trimestral'
                when product_id = 22125 then 'Pack Inmo 125 trimestral'
                when product_id = 23005 then 'Pack Inmo 5 semestral'
                when product_id = 23010 then 'Pack Inmo 10 semestral'
                when product_id = 23020 then 'Pack Inmo 20 semestral'
                when product_id = 23030 then 'Pack Inmo 30 semestral'
                when product_id = 23040 then 'Pack Inmo 40 semestral'
                when product_id = 23050 then 'Pack Inmo 50 semestral'
                when product_id = 23075 then 'Pack Inmo 75 semestral'
                when product_id = 23100 then 'Pack Inmo 100 semestral'
                when product_id = 23125 then 'Pack Inmo 125 semestral'
                when product_id = 24005 then 'Pack Inmo 5 anual'
                when product_id = 24010 then 'Pack Inmo 10 anual'
                when product_id = 24020 then 'Pack Inmo 20 anual'
                when product_id = 24030 then 'Pack Inmo 30 anual'
                when product_id = 24040 then 'Pack Inmo 40 anual'
                when product_id = 24050 then 'Pack Inmo 50 anual'
                when product_id = 24075 then 'Pack Inmo 75 anual'
                when product_id = 24100 then 'Pack Inmo 100 anual'
                when product_id = 24125 then 'Pack Inmo 125 anual'
              end as product_name,
              case when token_id is not null then 'Pack Manual'
                when payment_group_id is not null then 'Pack Online'
                else 'UNKNOWN' end as tipo_pack
            from
              packs
            )p on acc.account_id = p.account_id and rl.review_time < p.date_start
            left join
            (
              select
                pd.ad_id,
                case when max(pd.product_id) = 60 then 'Insertion Fee Job'
                when max(pd.product_id) = 61 then 'Insertion Fee Auto'
                when max(pd.product_id) = 62 then 'Insertion Fee Inmo'
                end as product_name,
                min(pu.receipt) as purchase_date,
                min(pd.price) as ifee_price
              from
                purchase pu
              join
                purchase_detail pd using(purchase_id)
              where
                pd.product_id in (60,61,62)
                and pu.status in ('paid','sent','confirmed')
              group by 1
              union all 
              select
                ad_id,
                case
                  when max(product_id) = 60 then 'Insertion Fee Job'
                  when max(product_id) = 61 then 'Insertion Fee Auto'
                  when max(product_id) = 62 then 'Insertion Fee Inmo'
                end as product_name,
                min(receipt_date) as purchase_date,
                min(price) as ifee_price
                from
                  purchase_in_app
                where
                  product_id in (60,61,62)
                  and status = 'confirmed'
                group by 1
          )lf
          using(ad_id)
          where
           rl.refusal_reason_text in ('Profesional INMO','Profesional Vehículos','Profesional Empleo')
           and rl.review_time::date
           between '""" + params.getDateFrom() + """'::date
           and '""" + params.getDateTo() + """'::date
        """
    dataEvasionDetails = rdbmsSource.selectToDict(
        queryEvasionModerationDetails)
    rdbmsEndpoint.copyEvasionDet(conf.getVal('ENDPOINTDB.TABLE.MED'),
                                 dataEvasionDetails)
    rdbmsSource.closeConnection()
    rdbmsEndpoint.closeConnection()
    te.getTime()
    logger.info('Process ended successed.')
