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


#filebackup - open this file
#filebackupoutput - result after change
#whatchange - row for change
#onchange - value that was in result after change

def UpdateFile(filebackup,filebackupoutput,whatchange,onchange):
    data = open(filebackup).read()
    o = open(filebackupoutput,'w')
    o.write( re.sub(whatchange,onchange,data) )
    o.close()

def ReturnCursorConnect():
    try :
        conn = my.connect(host=host, user=username, passwd=password, db=dbname,cursorclass=MySQLdb.cursors.DictCursor)
    except my.Error as e :
        print("error return connect function return connect = ",e)
    return conn

def SelectDataByVCID(conn,vzid):
    try :
        arrayres = []
        cursor = conn.cursor()
        queryfordatabase = regularpatsel + table + " WHERE vzid = " + vzid
	#print("query = ",queryfordatabase)
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
	#print("arrayres = ",arrayres)
        return arrayres
    except my.Error as e :
        print("error select data function select data = ",e)
    finally :
        conn.cursor().close()

#ctid, Mem, CpuCycles, CpuUsage, TpsRead, TpsWrite, VSU
def writeToRRD(input,ctid):
    rrdPath = '%s%s_temp.rrd' % (RRDsPath, ctid)
    #print("rrdPath = ",rrdPath)
    rrdFullPath = '%s%s.rrd' % (RRDsPath, ctid)
    #print("rrdFullPath = ",rrdFullPath)

    try :
        os.remove(rrdPath)
    except :
        print("error remove tmp rrd")

    try :
        os.remove(rrdFullPath)
    except :
        print("error remove main rrd")

    if not os.path.exists(rrdPath):
        try :
	     rrdtool.create(str(rrdPath), '--step', '300', '--start', '0',
                 'DS:us:GAUGE:600:U:U',
                 'DS:us_new:GAUGE:600:U:U',
                 'DS:mem:GAUGE:600:U:U',
                 'DS:tpswrite:GAUGE:600:U:U',
                 'DS:tpsread:GAUGE:600:U:U',
                 'DS:sigma:GAUGE:600:U:U',
                 'RRA:AVERAGE:0.5:1:105408',
                 'RRA:MAX:0.5:1:105408')
        except :
            print('Error, failed to create RRD for %s.' % (ctid))

    #backup file create
    filebackup = RRDsPath + ctid + "_backup.xml"
    command_createbackupfile = "rrdtool dump " + rrdPath + " " + filebackup
    #print("command create backup = ",command_createbackupfile)

    #delete file main
    command_delete = "rm " + rrdPath
    #print("command delete  = ",command_delete)


    #backupfile restore
    command_restorefombackup = "rrdtool restore " +  filebackup + " " + rrdPath
    #print("command restore backup = ",command_restorefombackup)

    #backupfile delete
    command_delete1 = "rm " + filebackup
    #print("command delete1  = ",command_delete1)

    try:
        code = os.popen(command_createbackupfile)
        now = code.read()
    except:
        print("can not make backupfile")
	sys.exit(1)

    #change field
    command_lastupdate = "rrdtool dump " + rrdPath + " | grep lastupdate"
    try:
        code = os.popen(command_lastupdate)
        now = code.read()
        now = now.replace("\r","")
        now = now.replace("\t","")
        now = now.replace("\n","")
	#print("now = ",now)

	#filebackup - open this file
	filebackupoutput = "/var/www/stats/rrd/" + ctid + "-output.xml"
        whatchange = now	#our lastupdate tag
        onchange = "<lastupdate>0</lastupdate>"
        UpdateFile(filebackup,filebackupoutput,whatchange,onchange) #update xml file for tag <lastupdate>
	commandrename = "mv " + filebackupoutput + " " + filebackup #rename
        #print("command rename = ",commandrename)
        try:
           code = os.popen(commandrename)
           now = code.read()
        except:
           print("can not make backupfile")
           sys.exit(1)

    except:
        print("can not change field")
        sys.exit(1)

    #delete main file
    try:
        code = os.popen(command_delete)
        now = code.read()
        #print("main rrd file was deleted")
    except:
        print("can not delete main file")
        sys.exit(1)

    #restore from backup
    try:
        code = os.popen(command_restorefombackup)
        now = code.read()
    except:
        print("couldnot restore main file frombackup")
        sys.exit(1)

    #delete backup file
    try:
        code = os.popen(command_delete1)
        now = code.read()
    except:
        print("can not delete backup file")
        sys.exit(1)

    for item in input:
	    #print("filebackup = ",filebackup)
	    #print("item = ",item)
            vzid_unparse = format(item['vzid'])           #vzid
            unixtime = format(item['unxtime'])            #unixtime
	    #print("unixtime = ",unixtime)
            mem_unparse = format(item['mem'])             #mem
            usnew_unparse = format(item['us_new'])        #cpu usage
            quota_unparse = format(item['quota'])         #space db
            unparse_tpsread = format(item['tpsread'])     #tpsread
            unparse_tpswrite = format(item['tpswrite'])   #tpswrite
            unparse_cpucycles = format(item['cpucycles']) #cpucycles
	    VSU = format(item['vzq'])			  #vsu
	    rrdParams = '%s:%s:%s:%s:%s:%s' % (unparse_cpucycles, usnew_unparse, mem_unparse,unparse_tpsread,unparse_tpswrite, VSU)
            rrdParams = unixtime + "@" + rrdParams
            #print("rrdparams = ",rrdParams)
	    try:
	       #print("UPDATE FILE = ",rrdPath)
               #print("RRD PARAM = ",rrdParams)
               command = "rrdtool update " + rrdPath  + " " + rrdParams
	       #print("command rrd update = ",command)
               try:
                   code = os.popen(command)
                   now = code.read()
	       except:
                   print("can not make rrd update")
            except:
    	        print("error update")

    #move tmd rrd to main rrd
    commandrenamerrd = "mv " + rrdPath + " " + rrdFullPath #rename
    #print("command rename = ",commandrenamerrd)
    try:
        code = os.popen(commandrenamerrd)
        now = code.read()
    except:
        print("can not make move rrd file")

#print("count params = ",len(sys.argv))
if len(sys.argv) < 2:
        print("Error Not all parameter")
        sys.exit(1)
vzid = sys.argv[1]
#print("vzid = ",vzid)
#print(vzid.isdigit())

if vzid.isdigit() == True:

   try :
       print("Eto chislo")
       myconnect = ReturnCursorConnect()
       array = SelectDataByVCID(myconnect,vzid)
       #print("answer size = ",len(array))
       writeToRRD(array,vzid)
   except :
       print("sql error at begin")
   finally :
       myconnect.close()

if vzid.isdigit() == False:
       print("Eto ne chislo")
       sys.exit(1)
