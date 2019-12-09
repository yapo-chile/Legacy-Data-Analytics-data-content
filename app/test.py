from infraestructure.conf import DBSource, DBEndpoint

dbs = DBSource()
dbe = DBEndpoint()
print(dbs.host)
print(dbe.host)