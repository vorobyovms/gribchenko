from xml.etree import ElementTree


tree = ElementTree.parse("/var/www/stats/rrd/888_backup1.xml")
w = tree.find('.//database')


#stringsss = "<row><v>43071</v><v>1.134</v><v>275533824</v><v>102400</v><v>2039808</v><v>1.863</v></row>"
#tree1 = ElementTree.fromstring(stringsss)
#row1 = ElementTree.SubElement(w, "row")
#row1.append(tree1)
#xmlstr = ElementTree.tostring(row1, encoding='utf8', method='xml')
#print("xms string = ",xmlstr)


data=[]
first=43071
data.append(float(first))
second=1
data.append(float(second))
third=275533824
data.append(float(third))
fourth=102400
data.append(float(fourth))
fifth=203980
data.append(float(fifth))
sixth=1
data.append(float(sixth))


row = ElementTree.SubElement(w, "row")
for item in data:
    print("item = ",item)
    v = ElementTree.SubElement(row,"v")
    item = "{0:.2f}".format(item)
    print item
    v.text = str(item)
tree.write(open('C1.xml', 'w'))
