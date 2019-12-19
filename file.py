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

filebackup="/var/www/stats/rrd/test.xml"
filebackup1="/var/www/stats/rrd/test1.xml"
pattern_find="<lastupdate>1576713600</lastupdate> <!-- 2019-12-19 00:00:00 UTC -->"
replace_pattern="<lastupdate>0</lastupdate>"

print("Begin")

o = open('output','a') #open for append
for line in open(filebackup):
   if re.match("^.+<lastupdate>", line) != None:
       print("Sovpalo!",line)
       new = line.strip().split('<lastupdate>')
       print("new = ",new)
       for ts in new:
           if ts == "":
              print("field")
           if ts != "":
              print("not empty")
              finalend = ts.strip().split('</lastupdate>')
              print("ts = ",ts)
       print("finalend = ",finalend)
       for ts1 in finalend:
           print(ts1)
           line = line.replace(ts1,'1')
	   o.write(line)
o.close()


#o = open('output','w')
#data = open(filebackup).read()
#o.write(re.sub(pattern_find,replace_pattern,data))
#o.close()

#f = open(filebackup)
#o = open('output','a')
#while 1:
#  line = f.readline()
#  if not line: break
#  line = line.replace("pattern_find",replace_pattern)
#  o.write(line + 'n')
#o.close()
