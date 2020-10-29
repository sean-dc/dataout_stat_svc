import mysql.connector
from singleton_meta import Singleton
from mysql.connector import pooling

# CONFIG(DB) READ BEGIN
from config_reader import Config

splus_config = Config().getConfig()
db_host = splus_config['DB']['HOST']
db_port = splus_config['DB']['PORT']
db_user = splus_config['DB']['USER']
db_pwd  = splus_config['DB']['PASSWORD']

# CONFIG(DB) READ END

config = {
  'pool_name': 'pynative_pool',
  'pool_size': 1, #5
  'pool_reset_session': True,
  'user':     db_user,
  'password': db_pwd,
  'host':     db_host,
  'port':     db_port,
  'database': 'cdc2r_management',
  'raise_on_warnings': True
}

class db_manager(metaclass=Singleton):
    """description of class"""
    def __init__(self):
        self.pool = pooling.MySQLConnectionPool(**config)
        #self.cnx = mysql.connector.connect(**config)
        
    def getConnection(self):
        return self.pool.get_connection()

    def __del__(self):
        pass

"""
Sample code
"""
def main():
    dbmanager = db_manager()
    for i in range(10):    
        cnx = dbmanager.getConnection()
        #cursor.execute("SELECT * FROM threatList")
        assetDict = {}

        alertMessage = "Alert"
        sourceIP = "172.16.51.111"
        destIP = "172.16.51.123123"
        assetInfo = assetDict.get(sourceIP, "Unknown")
        statusInfo =  "OPENED"

        insert_query = """INSERT INTO threatList(alertName, sourceIP, destIP, assetInfo, statusInfo) VALUES (%s, %s, %s, %s, %s)"""
        record = (alertMessage, sourceIP, destIP, assetInfo, statusInfo)

        cnx.cursor().execute(insert_query, record)
        cnx.commit()
        cnx.cursor().close()
        cnx.close()
        #dbmanager.getConnection().commit()

if __name__ == '__main__':
    main()    
