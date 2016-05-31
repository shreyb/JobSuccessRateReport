#!/home/sbhat/JSON_Queries/TestQueries/Test/bin/python

from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
from os import path,remove,mkdir
from shutil import rmtree
import logging


limit = 1000000000

#Files
outdir = 'JobRateOut'
writefile = outdir+'/20160509-20160510_gracc.out' 
errfile = outdir+'/20160509-20160510_gracc.err'
logfile = outdir+'/20160509-20160510.log'

files = [writefile,errfile,logfile]

if not path.exists(outdir):
	mkdir(outdir)

for file in files:
	if path.exists(file):
		remove(file)



with open(errfile,'w') as f:
	f.write("Check these records out using 'curl -XGET 'localhost:9200/INDEX/TYPE/ID''")

logging.basicConfig(filename=logfile)


client = Elasticsearch()

s = Search(using=client,index='gracc-osg-2016*').query(
    {
        "bool":{
          "must":[
            {"wildcard":{"VOName":"*dune*"}},
            {"wildcard":{"CommonName":"*dunegpvm01.fnal.gov"}}
          ],  
          "filter":[
              {"term":{"Resource.ResourceType":"BatchPilot"}},
              {"range":{
                "EndTime":{
                  "gte": "2016-05-09T12:01",
                  "lt":"2016-05-10T12:01"
                }   
              }}  
          ]   
        }   
      }  
#      "_source":["Resource.ResourceType","VOName","StartTime", "GlobalJobId","CommonName","EndTime", "Host.description","Resource.ExitCode"],
#
#      "sort":[
#        {"Host.description":{"order":"asc"}},
#        {"Host.value":{"order":"asc"}},
#        {"GlobalJobId":{"order":"asc"}},
#        {"Resource.ExitCode":{"order":"asc"}}
#      ],  
#      "size":5000   
#      
#    }
    )

totalcount=0
goodcount=0
badcount=0
with open(writefile,'w') as f:
	with open(errfile,'a') as f2:
		if totalcount < limit:
			for hit in s.scan():
				try:
					globaljobid = hit['GlobalJobId'][0]
					jobid = globaljobid.split('#')[1]+'@'+globaljobid[globaljobid.find('.')+1:globaljobid.find('#')]

					outstr= "%i\t%s\t%s\t%s\t%s\t%s\t%s\t%s" % (goodcount,hit['RecordId'][0],\
									hit['StartTime'][0],\
									hit['EndTime'][0],\
									jobid,\
									hit['Host']['description'][0],\
									hit['Host']['value'][0],\
									hit['Resource']['ExitCode'][0]
									)
					f.write(outstr)
					print outstr
					goodcount+=1
				#print hit.keys()
		#		for key in hit:
		#			print key
				
				#Next step:  handle this expection better
				except Exception as e:
					f2.write("\n%s\t%s" % (hit['RecordId'][0],hit))
					logging.exception(str(e))
					badcount+=1
				finally:
					totalcount+=1

print "%i lines returned without error" % goodcount
print "%i lines were written to the err file" % badcount
print "%i lines were returned in total" % totalcount
print "%s write complete." % writefile
print "%s write complete." % errfile




