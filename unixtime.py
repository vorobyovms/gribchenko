import unixtime
from datetime import datetime
import time

unixtime = "1544652000"
print("uniuxtime = ",unixtime)
ts = int(unixtime)
temptime = datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
comment = "<!-- " + temptime + " -->"
print("comment = ",comment)

