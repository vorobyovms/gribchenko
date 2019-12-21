import re
import json
import os
import sys
import urllib2
import MySQLdb as my
import MySQLdb.cursors
import json
import subprocess
import rrdtool
import datetime

host = "127.0.0.1"
dbname = "vtl"
table = "vzstat"
username = "vorobyov"
password = "rfmd2EAA1997"
regularpatsel = "SELECT vzq,tpsread,tpswrite,us,vzid,unxtime,mem,us_new,quota From "
update_vsu = "vzq"
time_field = "time"
global RRDsPath
global vctid
RRDsPath = '/var/www/stats/rrd/'

def getCpuNum():
  try:
    with open(os.devnull, 'w') as FNULL:
      out = subprocess.check_output(['/usr/bin/nproc'], stdin=None, stderr=FNULL, shell=False)
      return out.strip()
  except:
    print('Error, could not parse nproc output.')
    sys.exit(1)

def getCpuMHz():
    try:
        code = os.popen('lscpu | grep -ie "cpu mhz"')
	now = code.read()
	now = now.lower().replace(" ","")
	CpuMhz = now.split(':')[1].strip()
	return CpuMhz
    except:
       print('Error, could not parse lscpu output.')
       sys.exit(1)


def ReturnCursorConnect():
    try :
        conn = my.connect(host=host, user=username, passwd=password, db=dbname,cursorclass=MySQLdb.cursors.DictCursor)
    except my.Error as e :
        print("error return connect function return connect = ",e)
    return conn

def SelectData(conn,fdate,ldate):
    try :
	arrayres = []
        cursor = conn.cursor()
        queryfordatabase = regularpatsel + table + " WHERE DATE(" + time_field + ") BETWEEN " + "'" + fdate + "' AND " + "'" + ldate + "'"
        cursor.execute(queryfordatabase)
        rows = cursor.fetchall()
        for field in rows:
	    jsonlocal={}
            vzid = field['vzid']
            unixtime = field['unxtime']
            mem = field['mem']
            us_new = field['us_new']
            quota = field['quota']
	    tpsread = field['tpsread']
	    tpswrite = field['tpswrite']
            cpucycles = field['us']
	    jsonlocal["vzid"] = vzid
	    jsonlocal["unxtime"] = unixtime
	    jsonlocal["mem"] = mem
            jsonlocal["us_new"] = us_new
            jsonlocal["quota"] = quota
	    jsonlocal["tpsread"] = tpsread
            jsonlocal["tpswrite"] = tpswrite
	    jsonlocal["cpucycles"] = cpucycles
	    arrayres.append(jsonlocal)
	return arrayres
    except my.Error as e :
        print("error select data function select data = ",e)
    finally :
        conn.cursor().close()

def SelectDataByVCID(conn,vzid):
    try :
        arrayres = []
        cursor = conn.cursor()
        queryfordatabase = regularpatsel + table + " WHERE vzid = " + vzid
        cursor.execute(queryfordatabase)
        rows = cursor.fetchall()
        for field in rows:

            jsonlocal={}
            vzid = field['vzid']
            unixtime = field['unxtime']
            mem = field['mem']
            us_new = field['us_new']
            quota = field['quota']
            tpsread = field['tpsread']
            tpswrite = field['tpswrite']
            cpucycles = field['us']
	    vzq = field['vzq']

            jsonlocal["vzid"] = vzid
            jsonlocal["unxtime"] = unixtime
            jsonlocal["mem"] = mem
            jsonlocal["us_new"] = us_new
            jsonlocal["quota"] = quota
            jsonlocal["tpsread"] = tpsread
            jsonlocal["tpswrite"] = tpswrite
            jsonlocal["cpucycles"] = cpucycles
	    jsonlocal["vzq"] = vzq

            arrayres.append(jsonlocal)

        return arrayres
    except my.Error as e :
        print("error select data function select data = ",e)
    finally :
        conn.cursor().close()

def UpdateData(conn,sql):
    try:
	cursor = conn.cursor()
	cursor.execute(sql)
	conn.commit()
    except:
	print("error mysql update")
    finally:
        conn.cursor().close()


def FormulaUpdate(Mem,CpuUsage,Space,CpuNum,CpuMhz):
    try :
        SigMem = float(Mem) / 1024 / 1024 / 1024
        SigMemX = -0.00128996 * (SigMem ** 4) + 0.0618164 * (SigMem ** 3) - 0.657332 * (SigMem ** 2) + 4.04564 * SigMem - 1.12624
        SigCpu = (float(CpuUsage) / 100) * float(CpuNum) * (float(CpuMhz) / 1000) * 1.2
        SigCpuX = -0.000473002 * (SigCpu ** 4) + 0.0268319 * (SigCpu ** 3) - 0.0743712 * (SigCpu ** 2) + 0.939475 * SigCpu - 0.00936724
	SpaceGB = float(Space) / 1024 / 1024 / 1024
        SpaceQ = 5.34065 * (10 ** -9) * (SpaceGB) ** 4 - 4.67459 * (10 ** -6) * (SpaceGB ** 3) + 0.00113339 * (SpaceGB ** 2) + 0.180139 * SpaceGB - 0.535185
	VSU = max(SigMemX, SigCpuX, SpaceQ, 0)
	return VSU
    except :
        print("math error operation")

try:
    vctid = []
    CpuNum = getCpuNum()
    CpuMhz = getCpuMHz()
    date_format = '%Y-%m-%d'
    if len(sys.argv) < 3:
        print("Error Not all parameter")
        sys.exit(1)
    fdate = sys.argv[1]
    sdate = sys.argv[2]
    date_obj = datetime.datetime.strptime(fdate, date_format)
    date_obj1 = datetime.datetime.strptime(sdate, date_format)
    try :
        print("Begin")
        myconnect = ReturnCursorConnect()
        array = SelectData(myconnect,fdate,sdate)
        for item in array:
            vzid_unparse = format(item['vzid'])           #vzid
            unixtime = format(item['unxtime'])            #unixtime
            mem_unparse = format(item['mem'])             #mem
            usnew_unparse = format(item['us_new'])        #cpu usage
            quota_unparse = format(item['quota'])         #space db
	    unparser_tpsread = format(item['tpsread'])    #tpsread
	    unparse_tpswrite = format(item['tpswrite'])   #tpswrite
	    unparse_cpucycles = format(item['cpucycles']) #cpucycles
            VSU = FormulaUpdate(mem_unparse,usnew_unparse,quota_unparse)
            sqlupdate = "UPDATE " + table + " SET " + update_vsu + " = " + str(VSU) + " WHERE unxtime = " + unixtime + " AND vzid = " + vzid_unparse
            UpdateData(myconnect,sqlupdate)
    except :
        print("sql error at begin")
    finally :
        myconnect.close()

except ValueError:
    print("Incorrect data format, should be YYYY-MM-DD")
finally :
    sys.exit(1)
