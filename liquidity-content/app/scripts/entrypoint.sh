#!/bin/sh
# python script intended to fill database on db creation
# params :
# date_from=2019-12-31
# date_to=2019-12-31
# master=local

python /app/main.py -date_from=2020-07-17 -date_to=2020-07-20
#python /app/main.py $@