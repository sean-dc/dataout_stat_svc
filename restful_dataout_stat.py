from flask import Flask
from singleton_meta import Singleton
from flask_restful import reqparse, abort, Api, Resource
from db_manager import db_manager
import os
import logging  

MAX_ROW = 7

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

    def get_accessed_countries_stat(self, id):
        query = "SELECT * FROM insider_threat.dataout_access_country where do_id='{}' order by _id ;".format(id)
        logging.info(query)
        cnx = self.dbmanager.getConnection()
        accessed_countries = {}        
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
        return accessed_countries

    def get_dataout_class_stat(self, id):
        query = "SELECT * FROM insider_threat.dataout_class_stat where do_id='{}' order by _id ;".format(id)
        logging.info(query)
        cnx = self.dbmanager.getConnection()
        dataout_class_stat = {}
        try:            
            cursor = cnx.cursor(dictionary=True)
            cursor.execute(query)
            records = cursor.fetchall()
            row_count = 1
        
            labels = ['dst_port_count', 'app_proto_count', 'net_proto_count', 'rdp_ip_count', 'apt_event_count', 'waf_event_count', 'ips_event_count']
            data = {0:[0]*7, 1:[0]*7}
            for row in records:
                stat_type = row["type"]
                data[stat_type] = [ row['dst_port_count'],
                                                  row['app_proto_count'],
                                                  row['net_proto_count'],
                                                  row['rdp_ip_count'],
                                                  row['apt_event_count'],
                                                  row['waf_event_count'],
                                                  row['ips_event_count']]
                if row_count == MAX_ROW:
                    break
                
                row_count += 1
        
            dataout_class_stat['result'] = 'success'
            dataout_class_stat['labels'] = labels
            dataout_class_stat['data']   = data
            
        except Exception as e:
            dataout_class_stat['result'] = 'fail'
            dataout_class_stat['reason'] = str(e)
            logging.error('Error while fetching %s.(%s)', query, e)        
        finally:
            cnx.cursor().close()
            cnx.close()
        return dataout_class_stat

    def get_dataout_bytesin_avg_weekly_stat(self, id):
        query = "SELECT * FROM insider_threat.dataout_bytesin_avg_weekly  where do_id='{}' order by _id ;".format(id)
        logging.info(query)
        cnx = self.dbmanager.getConnection()
        dataout_bytesin_avg_weekly = {}
        try:            
            cursor = cnx.cursor(dictionary=True)
            cursor.execute(query)
            records = cursor.fetchall()
            row_count = 1
        
            labels = ['D-7', 'D-6', 'D-5', 'D-4', 'D-3', 'D-2', 'D-1']
            data = {0:[0]*7, 1:[0]*7}
            for row in records:
                stat_type = row["type"]
                data[stat_type] = [row['dminus7'],
                                   row['dminus6'],
                                   row['dminus5'],
                                   row['dminus4'],
                                   row['dminus3'],
                                   row['dminus2'],
                                   row['dminus1']]
                if row_count == MAX_ROW:
                    break
                
                row_count += 1
        
            dataout_bytesin_avg_weekly['result'] = 'success'
            dataout_bytesin_avg_weekly['labels'] = labels
            dataout_bytesin_avg_weekly['data']   = data
            
        except Exception as e:
            dataout_bytesin_avg_weekly['result'] = 'fail'
            dataout_bytesin_avg_weekly['reason'] = str(e)
            logging.error('Error while fetching %s.(%s)', query, e)        
        finally:
            cnx.cursor().close()
            cnx.close()
        return dataout_bytesin_avg_weekly

    def get_datout_bytesout_avg_weekly_stat(self, id):
        query = "SELECT * FROM insider_threat.dataout_bytesout_avg_weekly  where do_id='{}' order by _id ;".format(id)
        logging.info(query)
        cnx = self.dbmanager.getConnection()
        dataout_bytesout_avg_weekly = {}
        try:            
            cursor = cnx.cursor(dictionary=True)
            cursor.execute(query)
            records = cursor.fetchall()
            row_count = 1
        
            labels = ['D-7', 'D-6', 'D-5', 'D-4', 'D-3', 'D-2', 'D-1']
            data = {0:[0]*7, 1:[0]*7}
            for row in records:
                stat_type = row["type"]
                data[stat_type] = [row['dminus7'],
                                   row['dminus6'],
                                   row['dminus5'],
                                   row['dminus4'],
                                   row['dminus3'],
                                   row['dminus2'],
                                   row['dminus1']]
                if row_count == MAX_ROW:
                    break
                
                row_count += 1     
        
            dataout_bytesout_avg_weekly['result'] = 'success'
            dataout_bytesout_avg_weekly['labels'] = labels
            dataout_bytesout_avg_weekly['data']   = data
            
        except Exception as e:
            dataout_bytesout_avg_weekly['result'] = 'fail'
            dataout_bytesout_avg_weekly['reason'] = str(e)
            logging.error('Error while fetching %s.(%s)', query, e)        
        finally:
            cnx.cursor().close()
            cnx.close()
        return dataout_bytesout_avg_weekly

    def get_dataout_bytesinout_weekly_stat(self, id):
        query = "SELECT * FROM insider_threat.dataout_bytesinout_weekly where do_id='{}' order by _id ;".format(id)
        logging.info(query)
        cnx = self.dbmanager.getConnection()
        dataout_bytesinout_weekly = {}
        try:            
            cursor = cnx.cursor(dictionary=True)
            cursor.execute(query)
            records = cursor.fetchall()
            row_count = 1
        
            labels = ['D-7', 'D-6', 'D-5', 'D-4', 'D-3', 'D-2', 'D-1']
            data = {0:[0]*7, 1:[0]*7}
            for row in records:
                stat_type = row["type"]
                data[stat_type] = [row['dminus7'],
                                   row['dminus6'],
                                   row['dminus5'],
                                   row['dminus4'],
                                   row['dminus3'],
                                   row['dminus2'],
                                   row['dminus1']]
                if row_count == MAX_ROW:
                    break
                
                row_count += 1
            
            dataout_bytesinout_weekly['result'] = 'success'
            dataout_bytesinout_weekly['labels'] = labels
            dataout_bytesinout_weekly['data']   = data
            
        except Exception as e:
            dataout_bytesinout_weekly['result'] = 'fail'
            dataout_bytesinout_weekly['reason'] = str(e)
            logging.error('Error while fetching %s.(%s)', query, e)        
        finally:
            cnx.cursor().close()
            cnx.close()
        return dataout_bytesinout_weekly


    def get_dataout_additional_detection(self, id):
        query = "SELECT * FROM insider_threat.dataout_additional_detection where do_id='{}' order by _id ;".format(id)
        logging.info(query)
        cnx = self.dbmanager.getConnection()
        dataout_additional_detection = {}
        try:            
            cursor = cnx.cursor(dictionary=True)
            cursor.execute(query)
            records = cursor.fetchall()
            labels = ['Detection Time', 'Application', 'Leakeage Volumne']
            data = []
            for row in records:
                data.append([str(row['detection_time']), 
                             row['application'], 
                             row['volume']])
                
            dataout_additional_detection ['result'] = 'success'
            dataout_additional_detection ['labels'] = labels
            dataout_additional_detection ['data']   = data
            
        except Exception as e:
            dataout_additional_detection ['result'] = 'fail'
            dataout_additional_detection ['reason'] = str(e)
            logging.error('Error while fetching %s.(%s)', query, e)        
        finally:
            cnx.cursor().close()
            cnx.close()
        return dataout_additional_detection

    def  gather_dataout_stat(self, id):

        # [1] 국가 별 접속 통계
        accessed_countries = self.get_accessed_countries_stat(id)


        # [2] SRC IP VS 동일 대역 비교 통계
        dataout_class_stat = self.get_dataout_class_stat(id)

        
        # [3] 목적지 IP로 BytesIn 평균
        dataout_bytesin_avg_weekly = self.get_dataout_bytesin_avg_weekly_stat(id)

        
        # [4] 목적지 IP로 BytesOut 평균
        dataout_bytesout_avg_weekly = self.get_datout_bytesout_avg_weekly_stat(id)


        
        # [5] 데이터 유출 In/Out/Out-In 통계
        dataout_bytesinout_weekly = self.get_dataout_bytesinout_weekly_stat(id)

    
        # [6] 중복 탐지 리스트
        dataout_additional_detection = self.get_dataout_additional_detection(id)

        return {
            "accessed_countries"           : accessed_countries, 
            "dataout_class_stat"           : dataout_class_stat,
            "dataout_bytesin_avg_weekly"   : dataout_bytesin_avg_weekly,
            "dataout_bytesout_avg_weekly"  : dataout_bytesout_avg_weekly,
            "dataout_bytesinout_weekly"    : dataout_bytesinout_weekly,
            "dataout_additional_detection" : dataout_additional_detection
            }