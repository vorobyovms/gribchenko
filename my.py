import re
import json
import os
import sys
import urllib2
import MySQLdb as my
import MySQLdb.cursors
import json
import subprocess

host = "backup.pravogarant.com"
dbname = "vctest"
table = "vz"
username = "python"
password = "Pi1Ap4tWjX9CzRdk"
regularpatsel = "SELECT id,mem,us_new,quota From "
update_vsu = "gora"

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

def SelectData(conn):
    try :
	arrayres = []
        cursor = conn.cursor()
        queryfordatabase = regularpatsel + table
        cursor.execute(queryfordatabase)
        rows = cursor.fetchall()
        for field in rows:
	    jsonlocal={}
            id = field['id']
            mem = field['mem']
            us_new = field['us_new']
            quota = field['quota']
	    jsonlocal["id"] = id
	    jsonlocal["mem"] = mem
            jsonlocal["us_new"] = us_new
            jsonlocal["quota"] = quota
	    arrayres.append(jsonlocal)
	#return json.dumps(arrayres)
	return arrayres
    except my.Error as e :
        print("error select data function select data = ",e)
    finally :
        conn.cursor().close()

def UpdateData(conn,sql):
    try:
	cursor = conn.cursor()
	mycursor.execute(sql)
	conn.commit()
	print(mycursor.rowcount, "record(s) affected")
    except:
	print("error mysql update")
    finally:
        conn.cursor().close()


def FormulaUpdate(Mem,CpuUsage,Space):
    CpuNum = getCpuNum()
    CpuMhz = getCpuMHz()
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

try :
    myconnect = ReturnCursorConnect()
    array = SelectData(myconnect)
    print("array = ",array)

    #begin procedure update
    for item in array:
	id_unparse = format(item['id'])		#id
        mem_unparse = format(item['mem'])	#mem
        usnew_unparse = format(item['us_new'])  #cpu usage
	quota_unparse = format(item['quota']) 	#space db
        VSU = FormulaUpdate(mem_unparse,usnew_unparse,quota_unparse)
	print("VSU = ",VSU)
	sqlupdate = "UPDATE " + table + " SET " + update_vsu + " = " + str(VSU) + " WHERE "
	print("sqlupdate = ",sqlupdate)
except:
    print("sql error")

finally :
        myconnect.close()

