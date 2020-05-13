from flask import Flask
from singleton_meta import Singleton
from flask_restful import reqparse, abort, Api, Resource
from db_manager import db_manager
import os
import logging  

def get_rest_args_parser():
    parser = reqparse.RequestParser()
    parser.add_argument('id') # dataout_view record id
    #parser.add_argument('address')
    #parser.add_argument('status')
    return parser

class dataout_stat(Resource):
    def __init__(self):
        self.parser = get_rest_args_parser()
        self.dbmanager = db_manager()
        return

    def get(self, id):
        return self.gather_dataout_stat(id)

    def  gather_dataout_stat(self, id):
        accessed_countries = {}

        """
        수집(프로세스) : 로드밸런서, 로그스태시1~12
        """
        query = "SELECT * FROM insider_threat.dataout_access_country where do_id='{}' order by _id ;".format(id)
        logging.info(query)
        cnx = self.dbmanager.getConnection()
        try:            
            cursor = cnx.cursor(dictionary=True)
            cursor.execute(query)
            records = cursor.fetchall()

            for row in records:
                country = row["country"]
                count   = row["count"]
                accessed_countries[country] = count
        except Exception as e:
            logging.error('Error while fetching %s.(%s)', query, e)        
        finally:
            cnx.cursor().close()
            cnx.close()

        #"""
        #수집(유실) : 로드밸런서, 로그스태시1~12
        #"""
        #query = "SELECT * FROM collect_net_status order by _id desc limit 1;"
        #cnx = self.dbmanager.getConnection()
        #try:
        #    cursor = cnx.cursor(dictionary=True)    
        #    cursor.execute(query)
        #    records = cursor.fetchall()

        #    for row in records:
        #        net_status["LBLOSS"]   = row["lb_drop"]
        #        net_status["LS01LOSS"] = row["ls01_drop"]
        #        net_status["LS02LOSS"] = row["ls02_drop"]
        #        net_status["LS03LOSS"] = row["ls03_drop"]
        #        net_status["LS04LOSS"] = row["ls04_drop"]
        #        net_status["LS05LOSS"] = row["ls05_drop"]
        #        net_status["LS06LOSS"] = row["ls06_drop"]
        #        net_status["LS07LOSS"] = row["ls07_drop"]
        #        net_status["LS08LOSS"] = row["ls08_drop"]
        #        net_status["LS09LOSS"] = row["ls09_drop"]
        #        net_status["LS10LOSS"] = row["ls10_drop"]
        #        net_status["LS11LOSS"] = row["ls11_drop"]
        #        net_status["LS12LOSS"] = row["ls12_drop"]
        #except Exception as e:
        #    logging.error('Error while fetching %s.(%s)', query, e)        
        #finally:
        #    cnx.cursor().close()
        #    cnx.close()

        #"""
        #전송 : 카프카1~3 프로세스 상태, 파티션0~5 지연 상태
        #"""
        #query = "SELECT * FROM cdc2r_management.trasmit_kafka_status order by _id desc limit 1;"
        #cnx = self.dbmanager.getConnection()
        #try:
        #    cursor = cnx.cursor(dictionary=True)    
        #    cursor.execute(query)
        #    records = cursor.fetchall()
            
        #    for row in records:
        #        pss_status["Kafka01"]    = row["kafka_01_status"]
        #        pss_status["Kafka02"]    = row["kafka_02_status"]
        #        pss_status["Kafka03"]    = row["kafka_03_status"]
        #        net_status["Partition00"] = row["partition_fw00_lag"]
        #        net_status["Partition01"] = row["partition_fw01_lag"]
        #        net_status["Partition02"] = row["partition_fw02_lag"]
        #        net_status["Partition03"] = row["partition_fw03_lag"]
        #        net_status["Partition04"] = row["partition_fw04_lag"]
        #        net_status["Partition05"] = row["partition_fw05_lag"]

        #except Exception as e:
        #    logging.error('Error while fetching %s.(%s)', query, e)        
        #finally:
        #    cnx.cursor().close()
        #    cnx.close()

        #"""
        #전송 : 로그 스태시 1~6 프로세스 상태
        #"""
        #query = "SELECT * FROM cdc2r_management.trasmit_logstash_fw_status order by _id desc limit 1;"
        #cnx = self.dbmanager.getConnection()
        #try:
        #    cursor = cnx.cursor(dictionary=True)    
        #    cursor.execute(query)
        #    records = cursor.fetchall()            
        #    for row in records:
        #        pss_status["LSFW01"] = row["logstash_fw01_status"]
        #        pss_status["LSFW02"] = row["logstash_fw02_status"]
        #        pss_status["LSFW03"] = row["logstash_fw03_status"]
        #        pss_status["LSFW04"] = row["logstash_fw04_status"]
        #        pss_status["LSFW05"] = row["logstash_fw05_status"]
        #        pss_status["LSFW06"] = row["logstash_fw06_status"]

        #except Exception as e:
        #    logging.error('Error while fetching %s.(%s)', query, e)        
        #finally:
        #    cnx.cursor().close()
        #    cnx.close()

        #"""
        #저장 : 엘라스틱 상태
        #"""        
        #query = "SELECT * from esnode_status where node_id in (SELECT distinct(node_id) from esnode_status where collect_time >= DATE_SUB(NOW(),INTERVAL 1 HOUR)) and collect_time >= DATE_SUB(NOW(),INTERVAL 1 HOUR) order by node_id;"
        #cnx = self.dbmanager.getConnection()
        #try:
        #    cursor = cnx.cursor(dictionary=True)    
        #    cursor.execute(query)
        #    records = cursor.fetchall()            
        #    for row in records:
        #        node_id = row["node_id"]                
        #        pss_status[node_id] = "active"

        #except Exception as e:
        #    logging.error('Error while fetching %s.(%s)', query, e)        
        #finally:
        #    cnx.cursor().close()
        #    cnx.close()

        return {"accessed_countries" : accessed_countries, "dataout_stat": {}}