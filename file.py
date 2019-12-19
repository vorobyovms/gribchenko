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
pattern_find="<lastupdate>1576713600</lastupdate> <!-- 2019-12-19 00:00:00 UTC -->"
replace_pattern="<lastupdate>0</lastupdate>"

f = open(filebackup).read()
f = f.replace(pattern_find,replace_pattern)
