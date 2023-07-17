import json
import os
import re
import subprocess
import pandas as pd
import boto3
import botocore
from dotenv import load_dotenv
import psycopg2


class main():

    def __int__(self):
        load_dotenv()

    def checkFile(self):
        fileStats = {}
        counterCorrect = 0
        total = 0
        EMAIL_REGEX = re.compile(r'[^@]+@[^@]+\.[^@]+')
        file = open("../data/aid_32297_za434l.csv", 'r')
        fileSize = os.stat("../data/aid_32297_za434l.csv").st_size / (1024 * 1024)
        if fileSize > 4:
            print("file Size is greater than 4 GB actual size is {0}".format(fileSize))
        for count,lines in enumerate(file.read().split('\n')):
            total += 1
            if  EMAIL_REGEX.match(lines):
                counterCorrect += 1
        if total <= 1:
            print("count of emails in file is {0}".format(total))
        if counterCorrect < int(total/2):
            print("total records in file is {0} and correct emails count is {1}".format(total,counterCorrect))
        fileStats["fileName"] = "data/aid_32297_za434l.csv".split("/")[1]
        fileStats["fileSize"] = fileSize
        fileStats["totalCount"] = total
        fileStats["CorrectEmailCount"] = counterCorrect
        fileStats["InCorrectEmailCount"] = total - counterCorrect
        print(fileStats)


    def getFileInfo(self, missing=[]):
        load_dotenv()
        global fileName
        sqlresults = []
        with open("../data/query.sql", "r") as sql_file:
            query = sql_file.read()
        results  = self.connectToPostgres(os.environ['CORE_HOST'], os.environ['CORE_USER'], os.environ['CORE_PW'],os.environ['CORE_PORT'], query)
        files = results.fetchall()
        for recs in files:
            if recs is not None:
                sqlresults.append(recs[2])
        curls = self.runCurl()
        resp = []
        for keys ,values in curls.items():
            resp.append(keys)
        for data in sqlresults:
            if data not in resp:
                missing.append(data)
        return missing


    def connectToS3(self):
        s3 = boto3.client('s3')
        bucket_name = 'sh-dw-external-tables-prod'
        # bucket_name = 'sh-dw-external-tables-dev'
        file_name = 'crm_original/aid_33702_p870h.csv'
        # file_name = 'crm/aid_11500_IP_1828.csv'
        obj = s3.get_object(Bucket=bucket_name, Key=file_name)
        print(obj['Body'].read().decode('utf-8'))

    def runCurl(self):
        output = subprocess.check_output(['curl', '-H', 'Content-Type: application/json', '-H',
                                        'Host: membership-gateway-prod.coreprod.west2.steelhouse.com',
                                        'http://a48e0dc5459e74ed9a460f718d19f2e8-a863d546fb12e42c.elb.us-west-2.amazonaws.com/totals'])

        data = json.loads(output.decode())
        with open("../data/tmp.json", 'w') as file:
            json.dump(data,file)
        retArr = {}
        with open("../data/tmp.json") as jfile:
            json_data = jfile.read()
            data = json.loads(json_data)
            for key, value in data.get("data_source_counts").items():
                for keys,values in data.get("data_source_counts").get(key).get("counts").items():
                    retArr[int(keys)] = values
        return retArr


    def connectToPostgres(self,dburl, user, passd, port, query):
        conn = psycopg2.connect(database="coredw", user=user, password=passd, host=dburl, port=port)
        cursor = conn.cursor()
        cursor.execute(query)
        return cursor


    def generatereport(self):
        load_dotenv()
        conn = psycopg2.connect(database="coredw", user=os.environ['CORE_USER'], password=os.environ['CORE_PW'], host=os.environ['CORE_HOST'], port=os.environ['CORE_PORT'])
        with open("../data/query.sql", "r") as sql_file:
            query = sql_file.read()
        df = pd.read_sql(query, conn)
        df['create_time'] = pd.to_datetime(df['create_time'])
        df['create_time'] = df['create_time'].dt.strftime('%Y-%m-%d %H:%M:%S')
        src_mappings = self.runCurl()
        print(type(src_mappings))
        data = list(src_mappings.items())
        df_cur = pd.DataFrame(data,columns=['data_source_category_id','household_Count'])
        merged_df = pd.merge(df, df_cur, on='data_source_category_id',how='left')
        merged_df["percentage_match"] = (merged_df["household_Count"]/merged_df["entry_count"])*100
        merged_df["List Issue Types"] = ''
        merged_df["notes"] = ''
        print(merged_df)
        merged_df.to_excel('../data/output.xlsx', index=False)

    # def addComments(self,row):
    #     if pd.isnull(row['household_count']):
    #         return  'Missing Household count'
    #     return ''

    # def Add


    def refreshSecurityToken(self):
        p = subprocess.Popen(['okta-awscli', '--profile', 'core', '--okta-profile', 'core'])
        print(p.communicate())

if __name__ == '__main__':
    main().generatereport()


# change the query to get the list for last 30 days

