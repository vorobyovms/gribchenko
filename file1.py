import re
data = open('file').read()
o = open('output','w')
o.write( re.sub('misha','ura',data) )
o.close()
