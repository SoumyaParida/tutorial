import csv
import subprocess

#curl http://0.0.0.0:6800/schedule.json -d project=default -d spider=alexa
import urllib2
cmd = ''' curl http://0.0.0.0:6800/schedule.json -d project=default -d spider=alexa'''
args = cmd.split()
process = subprocess.Popen(args, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
stdout, stderr = process.communicate()

def merge_dicts(*dict_args):
    '''
    Given any number of dicts, shallow copy and merge into a new dict,
    precedence goes to key value pairs in latter dicts.
    '''
    result = {}
    for dictionary in dict_args:
        result.update(dictionary)
    return result

csvinput = open("output6.csv",'r')
csv_f=csv.reader(csvinput, delimiter='\t',quotechar=' ')
csvoutput=open("finalOutput.csv", 'w')
writer = csv.writer(csvoutput,delimiter='\t',quotechar=' ')

checked_list = []
d = dict() 
index_set = set()
for row in csv_f:
    index_set.add(row[0])


for idx in index_set:
    csvinput.seek(0)
    unique_asn = set()
    for row in csv_f:
        if row[0] == idx:
            #print "row",row[4]
            if '-' not in row[9]:
                try:
                    if ';' in row[9]:
                        rowASN=row[9].split(";")
                        for asn in rowASN:
                            unique_asn.add(asn)
                    else:
                        unique_asn.add(row[9])
                except:
                    print "row=row"
    #print "ASN number change for index %s is : %s %s" % (str(idx), str(len(unique_asn)),unique_asn)    
    
    with open("output6.csv",'r') as inputfile:
        for row in inputfile:
            field = row.strip().split('\t')
            if field[0]==idx:
                field.insert(10,str(len(unique_asn)))
                writer.writerow(field)

csvfile=open('log.csv')
fieldnames = ['url', 'counter','ExternalImageCount','InternalImageCount','ExternalscriptCount','InternalscriptCount','ExternallinkCount','InternallinkCount','ExternalembededCount','InternalembededCount','UniqueExternalSites','ExternalSites']
reader = csv.DictReader(csvfile,fieldnames=fieldnames)

outputCSV=open("finalOutput.csv", 'r')
readerCSV = csv.reader(outputCSV,delimiter='\t',quotechar=' ')

finaloutput=open("final.csv", 'wbr+')
writerOutput = csv.writer(finaloutput,delimiter='\t',quotechar=' ',quoting=csv.QUOTE_MINIMAL)
keyValuePairLine = { }
keyValuePairLineNew= { }
for row in reader:
    key=str(row['counter']+row['url'])
    keyValuePairLine.setdefault(key, []).append(row)
#print keyValuePairLine

for key,value in keyValuePairLine.items():
    for Valuedict in value:
        empty_keys = [k for k,v in Valuedict.iteritems() if not v or v=='[]']
        for k in empty_keys:
            del Valuedict[k]

for key,value in keyValuePairLine.items():
    value1=merge_dicts(value[0],value[1],value[2],value[3]) 
    keyValuePairLineNew[key]=value1


for row in outputCSV:
    TotalInternalObjects=TotalExternalObjects=0
    count=0
    field = row.strip().split('\t')
    keyValuepaircheck=field[0]+field[4]
    if keyValuepaircheck in keyValuePairLineNew:
        value=keyValuePairLineNew[keyValuepaircheck]
        TotalExternalObjects= int(value['ExternalImageCount'])+int(value['ExternalscriptCount'])+int(value['ExternallinkCount'])+int(value['ExternalembededCount'])
        TotalInternalObjects=int(value['InternalImageCount'])+int(value['InternalscriptCount'])+int(value['InternallinkCount'])+int(value['InternalembededCount'])
    else:
        TotalExternalObjects=0
        TotalInternalObjects=0

    if keyValuepaircheck in keyValuePairLine:
        value=keyValuePairLine[keyValuepaircheck]
        for item in value:
            count=count+int(item['UniqueExternalSites'])
    field.insert(11,TotalExternalObjects)
    field.insert(12,TotalInternalObjects)
    field.insert(13,count)
    writerOutput.writerow(field)