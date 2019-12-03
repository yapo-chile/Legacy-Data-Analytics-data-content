import sys
import logging
from utils.readParams import readParams
from utils.configuration import conf
from utils.spark import spark
from utils.database import database

if __name__ == '__main__':
    logger = logging.getLogger('content-evasion-moderation')
    format = """%(asctime)s,%(msecs)d %(levelname)-2s [%(filename)s:%(lineno)d] %(message)s"""
    logging.basicConfig(format=format,
                        level=logging.INFO)

    params = readParams(sys.argv)
    configuration = conf(params.getConfigurationFile())
    rdbms = database(configuration.getSpecific('ENDPOINT.HOST'),
                      configuration.getSpecific('ENDPOINT.PORT'),
                      configuration.getSpecific('ENDPOINT.DATABASE'),
                      configuration.getSpecific('ENDPOINT.USERNAME'),
                      configuration.getSpecific('ENDPOINT.PASSWORD'))
    rdbms.executeCommand()
    appName = 'content-evasion-moderation-child'
    queryEvasionModeration = """
    ( select
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
                and rl.review_time::date between '"""+params.getDate1()+"""'::date and '"""+params.getDate2()+"""'::date
              group by
              4,5 ) em
      """

    spark = spark(appName, params.getMaster())
    dataFrame = spark.getSparkSql(
        configuration.getSpecific('BLOCKETDB.HOST'),
        configuration.getSpecific('BLOCKETDB.PORT'),
        configuration.getSpecific('BLOCKETDB.DATABASE'),
        configuration.getSpecific('BLOCKETDB.USERNAME'),
        configuration.getSpecific('BLOCKETDB.PASSWORD'),
        queryEvasionModeration)
    spark.stopSparkSession()
    logger.info('Process ended successed.')
