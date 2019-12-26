import time
import datetime
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
import fileinput
from xml.etree import ElementTree

#------------------------------------------------------------------------peremennie
print("begin = ",datetime.datetime.now().time())
host = "127.0.0.1"									#database ip or hostname
dbname = "vtl"          								#database name
table = "vzstat"        								#table for select
username = "vorobyov"									#username for database
password = "rfmd2EAA1997"								#password
regularpatsel = "SELECT vzq,tpsread,tpswrite,us,vzid,unxtime,mem,us_new,quota From "    #part of sql query
update_vsu = "vzq"									#field for update vsu in table
time_field = "time"									#field time in database
global RRDsPath										#RRD Path
RRDsPath = '/var/www/stats/rrd/'
global vctid										#id container

#--------------------------------------------------------------------------return connect for database--------------------------------------------------------------------
def ReturnConnect():
    try :
        conn = my.connect(host=host, user=username, passwd=password, db=dbname,cursorclass=MySQLdb.cursors.DictCursor)
    except my.Error as e :
        print("error return connect function return connect = ",e)
    return conn


#-------------------------------------------------------------------------this function return result (result as class cursor) query for container that we send in this functiom
# params in
#1) connection
#2) container id
def SelectDataByVCID(conn,vzid):
    try :
        arrayres = []
        cursor = conn.cursor()
        queryfordatabase = regularpatsel + table + " WHERE vzid = " + vzid + " ORDER BY unxtime"
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

#-----------------------------------------------------------------------------delete xml file - udalyaem file pered tem kak sozdadim noviy
def DeleteXMLFile(vzctid):
    deletepath_for_xmlfile = RRDsPath + vzctid+".xml"
    try :
        os.remove(deletepath_for_xmlfile)
    except :
        print("can't delete file ",deletepath_for_xmlfile)

#------------------------------------------------------------------------------create xml file and return his name if not catch
def CreateXMLFile(vzctid):
    filename = RRDsPath + vzctid +".xml"
    try:
        open(filename, 'w').close()
    except:
        print("Can't create file XML")
        sys.exit(1)
    return filename

#-------------------------------------------------------------------------------FORMIROVANIE STRUKTURI XML FILE (tut na vhod daem  imya file kotoriy bil sozdan) -------------
def FormatStructXMLFile(filename):
    print "call function FormatStructXMLFile()"

    #rrd => ds
    #    => <rra>

    #_______________________RRD TAGS________________________________________________
    rrd = {}
    rrd["version"] = "003"
    rrd["step"] = "300"
    rrd["lastupdate"] = "0"

    #___________________________TAGS DS__________________________________________

    all_ds = []
    paramsname = ["us","us_new","mem","tpswrite","tpsread","sigma"]
    print("params name size = ",len(paramsname))
    a = 0
    while a < len(paramsname):
        dstemp = {}
        dstemp["name"] = paramsname[a]
        dstemp["type"] = "GAUGE"
        dstemp["minimal_heartbeat"] = "600"
        dstemp["min"] = "NaN"
        dstemp["max"] = "NaN"
        dstemp["comment"] = "<!-- PDP Status -->"
        dstemp["last_ds"] = "U"
        dstemp["value"] = "NaN"
        dstemp["unknown_sec"] = "0"
        all_ds.append(dstemp)
        a = a + 1

    rrd["ds"] = all_ds #put array ds in json rrd
    print("result rrd = ", rrd)

    #_______________________RRA TAGS ______________________________________________
    #rra
    #  cf
    #  pdp_per_row
    #  params
    #  cdp_prep
    #  database

    rra = {}

    #2)
    rra["cf"] = "MAX"
    rra["pdp_per_row"] = "1"

    #3) Params
    params = {}
    params["xff"] = "NaN"
    rra["params"] = params

    #4) CDP
    cdp_prep_array = []
    i = 0
    while i < len(all_ds):
        cdp_prep = {}
        cdp_prep_withkey = {}
        cdp_prep["primary_value"] = "0.0000000000e+00"
        cdp_prep["secondary_value"] = "0.0000000000e+00"
        cdp_prep["value"] = "NaN"
        cdp_prep["unknown_datapoints"] = "0"
        cdp_prep_withkey["ds"] = cdp_prep
        cdp_prep_array.append(cdp_prep_withkey)
        i = i + 1
    rra["cdp_prep"] = cdp_prep_array

    #5) database
    database = {}
    database_array = []
    rra["database"] = database_array
    print("rra = ",rra)

    rrd["rra"] = rra
    print("rrd = ",rrd)

#---------------------------------------------------------------------------------------------------NACHALO --------------------------------------------------------------------
if len(sys.argv) < 2:								#NA VHOD podaem nomer conteynera
        print("Error Not all parameter")
        sys.exit(1)
vzid = sys.argv[1]

if vzid.isdigit() == True:

   try :
       print("Eto chislo")
       myconnect = ReturnConnect() 				                     #Vizavaem viborku select s tablici
       array = SelectDataByVCID(myconnect,vzid)                                      #poluchaem resultat
       print("select size = ",len(array))                                            #pechataem dlinnu polichennogo
       print("SQL QUERY FINISHED = ",datetime.datetime.now().time())                 #pechataem vremya tekushee - dlya togo chtobi ponimat skolko proshlo vremeni
       DeleteXMLFile(vzid)							     #udalyaem file xml
       filename = CreateXMLFile(vzid)					             #sozdaem file xml
       print("after delete and create empty file ",datetime.datetime.now().time())
       if filename != "":
           FormatStructXMLFile(filename)
   except :
       print("sql error at begin")
   finally :
       myconnect.close()

