import sys
import os
import re
from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q

import optparse
import traceback
import TextUtils
import Configuration
from Reporter import Reporter



class Jobs:
    def __init__(self):
        self.jobs = {}

    def add_job(self, site, job):
        if not self.jobs.has_key(site):
            self.jobs[site] = []

        self.jobs[job.site].append(job)

class Job:
    def __init__(self, end_time, start_time, jobid, site, host, exit__code):
        self.end_time = end_time
        self.start_time = start_time
        self.jobid = jobid
        self.site = site
        self.host = host
        self.exit_code = exit__code


class JobSuccessRateReporter(Reporter):
    def __init__(self, configuration, start, end, vo, template, is_test, verbose):
        Reporter.__init__(self, configuration, start, end, verbose)
        self.is_test = is_test
        self.vo = vo
        self.template = template
        self.title = "Production Jobs Success Rate %s - %s" % (self.start_time, self.end_time)
        self.run = Jobs()
        self.clusters = {}
        self.connectStr = None

    def generate(self):
	client=Elasticsearch(['https://gracc.opensciencegrid.org/e'],use_ssl=True,verify_certs=False,client_cert='gracc_cert/gracc-reports-dev.crt',client_key='gracc_cert/gracc-reports-dev.key',timeout=60)
	#client=Elasticsearch(timeout=60)
        results=[]
	
 	common_name = self.config.get("query", "%s_commonname" % (self.vo.lower()))
	wildcardvoq = '*'+self.vo.lower()+'*'
	wildcardcommonnameq ='*'+common_name+'*'
	
	start_date = re.split('[/ :]', self.start_time)
	starttimeq = datetime(int(start_date[0]),int(start_date[1]),int(start_date[2]),int(start_date[3]),int(start_date[4])).isoformat()
	
	end_date = re.split('[/ :]', self.end_time)
	endtimeq = datetime(int(end_date[0]),int(end_date[1]),int(end_date[2]),int(end_date[3]),int(end_date[4])).isoformat()
	
	#querystringverbose = '{"bool":{"must":[{"wildcard":{"VOName":"%s"}},{"wildcard":{"CommonName":"%s"}}],"filter":[{"term":{"Resource.ResourceType":"BatchPilot"}},{"range":{"EndTime":{"gte": "%s","lt":"%s"}}}]}}' % (wildcardvoq,wildcardcommonnameq,starttimeq,endtimeq)

	resultset = Search(using=client,index='gracc.osg.raw*') \
        	.query("wildcard",VOName=wildcardvoq)\
        	.query("wildcard",CommonName=wildcardcommonnameq)\
        	.filter("range",EndTime={"gte":starttimeq,"lt":endtimeq})\
        	.filter(Q({"term":{"ResourceType":"Payload"}}))	
	

	querystringverbose=resultset.to_dict()	

	response = resultset.execute()
	return_code_success = response.success()	#True if the elasticsearch query completed without errors
        
	
	for hit in response:
	    print hit.to_dict()['RecordId']
	
	for hit in resultset.scan():
            try:
		globaljobid = hit['GlobalJobId']
		jobid = globaljobid.split('#')[1]+'@'+globaljobid[globaljobid.find('.')+1:globaljobid.find('#')]
		outstr= "%s\t%s\t%s\t%s\t%s\t%s" % (hit['StartTime'],\
						hit['EndTime'],\
						jobid,\
						hit['Host_description'],\
						hit['Host'],\
						hit['Resource_ExitCode']
						)
		results.append(outstr)

		if self.verbose:
			print >> sys.stdout, outstr
            except KeyError as e:
                pass #Figure this out
	
	
        #mysql_client_cfg = MySQLUtils.createClientConfig("main_db", self.config)
        #self.connectStr = MySQLUtils.getDbConnection("main_db", mysql_client_cfg, self.config)
        #common_name = self.config.get("query", "%s_commonname" % (self.vo.lower()))
        #select = "select StartTime, EndTime, CONCAT(substring_index(substring(GlobalJobId, 28), '#', 1), '@', " + \
        #         "substring_index(substring(GlobalJobId, 8), '#', 1)), HostDescription, substring_index(Host," + \
        #         "' ', 1), r.Value as Status  from JobUsageRecord j,  Resource r where r.dbid = j.dbid and" + \
        #         " r.Description = 'ExitCode' and  EndTime>= '" + self.start_time + "' and EndTime < '" + \
        #         self.end_time + "' and ResourceType = 'BatchPilot' and CommonName like '%" + common_name + \
        #         "%' and VOName like '%" + self.vo.lower() + "%' order by HostDescription, Host, GlobalJobId,  r.Value;"
        if self.verbose:
            print >> sys.stdout, querystringverbose
        #results, return_code = MySQLUtils.RunQuery(select, self.connectStr)
        if not return_code_success:
            raise Exception('Error to access mysql database')
        
	#Replaced with print statement in resultset.scan() loop
	#if self.verbose:
        #    print >> sys.stdout, results

        if len(results) == 1 and len(results[0].strip()) == 0:
            print >> sys.stdout, "Nothing to report"
            return
        
    
    	for line in results:
            tmp = line.split('\t')
            start_time = tmp[0].strip().replace('T',' ').replace('Z','')
            end_time = tmp[1].strip().replace('T',' ').replace('Z','')
            jobid = tmp[2].strip()
            site = tmp[3].strip()
            if site == "NULL":
                continue
            host = tmp[4].strip()
            status = int(tmp[5].strip())
            job = Job(end_time, start_time, jobid, site, host, status)
            self.run.add_job(site, job)
            clusterid = jobid.split(".")[0]
            if not self.clusters.has_key(clusterid):
                self.clusters[clusterid] = []
            self.clusters[clusterid].append(job)
	    
#        MySQLUtils.removeClientConfig(mysql_client_cfg)

    def send_report(self):
        table = ""
        total_jobs = 0
        total_failed = 0
        if len(self.run.jobs) == 0:
            return
        table_summary = ""
        job_table = ""
        for cid, jobs in self.clusters.items():
            total_jobs = len(jobs)
            failures = []
            total_jobs_failed = 0
            for job in jobs:
                if job.exit_code == 0:
                    continue
                total_jobs_failed += 1
                failures.append(job)
            if total_jobs_failed == 0:
                continue
            job_table += '\n<tr><td align = "left">%s</td><td align = "right">%s</td><td align = "right">%s</td><td></td><td></td><td></td><td></td><td></td><td></td></tr>' \
                % (cid, total_jobs, total_jobs_failed,)
            for job in failures:
                job_table += '\n<tr><td></td><td></td><td></td><td align = "left">%s</td><td align = "left">%s</td><td align = "left">%s</td><td align = "right">%s</td><td align = "right">%s</td><td align = "right">%s</td></tr>' % \
                             (job.jobid, job.start_time, job.end_time, job.site, job.host, job.exit_code)
        total_jobs = 0

        for key, jobs in self.run.jobs.items():
            failed = 0
            total = len(jobs)
            failures = {}
            for job in jobs:
                if job.exit_code != 0:
                    failed += 1
                    if not failures.has_key(job.host):
                        failures[job.host] = {}
                    if not failures[job.host].has_key(job.exit_code):
                        failures[job.host][job.exit_code] = 0
                    failures[job.host][job.exit_code] += 1
            total_jobs += total
            total_failed += failed
            table_summary += '\n<tr><td align = "left">%s</td><td align = "right">%s</td><td align = "right">%s</td><td align = "right">%s</td></tr>' % \
                             (key, total, failed, round((total - failed) * 100. / total, 1))
            table += '\n<tr><td align = "left">%s</td><td align = "right">%s</td><td align = "right">%s</td><td align = "right">%s</td><td></td><td></td><td></td></tr>' % \
                     (key, total, failed, round((total - failed) * 100. / total, 1))
            for host, errors in failures.items():
                for code, count in errors.items():
                    table += '\n<tr><td></td><td></td><td></td><td></td><td align = "left">%s</td><td align = "right">%s</td><td align = "right">%s</td></tr>' \
                        % (host, code, count)

        table += '\n<tr><td align = "left">Total</td><td align = "right">%s</td><td align = "right">%s</td><td align = "right">%s</td><td></td><td></td><td></td></tr>' % \
                 (total_jobs, total_failed, round((total_jobs - total_failed) * 100. / total_jobs, 1))
        table_summary += '\n<tr><td align = "left">Total</td><td align = "right">%s</td><td align = "right">%s</td><td align = "right">%s</td></td></tr>' % \
                         (total_jobs, total_failed, round((total_jobs - total_failed) * 100. / total_jobs, 1))
        text = "".join(open(self.template).readlines())
        text = text.replace("$START", self.start_time)
        text = text.replace("$END", self.end_time)
        text = text.replace("$TABLE_SUMMARY", table_summary)
        text = text.replace("$TABLE_JOBS", job_table)
        text = text.replace("$TABLE", table)
        text = text.replace("$VO", self.vo)
        fn = "%s-jobrate.%s" % (self.vo.lower(), self.start_time.replace("/", "-"))
        

	f = open(fn, "w")
        f.write(text)
        f.close()
	
	#The part that actually emails people.  Will need to figure out why this didn't work.
	if self.is_test:
            emails = self.config.get("email", "test_to").split(", ")
        else:
            emails = self.config.get("email", "%s_email" % (self.vo.lower())).split(", ") + \
                     self.config.get("email", "test_to").split(", ")
        TextUtils.sendEmail(([], emails), "%s Production Jobs Success Rate on the OSG Sites (%s - %s)" %
                            (self.vo, self.start_time, self.end_time), {"html": text},
			    ("Gratia Operation", "tlevshin@fnal.gov"), "localhost")

        os.unlink(fn)


def parse_opts():
    """Parses command line options"""

    usage = "Usage: %prog [options]"
    parser = optparse.OptionParser(usage)
    parser.add_option("-c", "--config", dest="config", type="string",
                      help="report configuration file (required)")
    parser.add_option("-v", "--verbose",
                      action="store_true", dest="verbose", default=False,
                      help="print debug messages to stdout")
    parser.add_option("-E", "--experiement",
                      dest="vo", type="string",
                      help="experiment name")
    parser.add_option("-T", "--template",
                      dest="template", type="string",
                      help="template_file")
    parser.add_option("-s", "--start", type="string",
                      dest="start", help="report start date YYYY/MM/DD HH:MM:DD (required)")
    parser.add_option("-e", "--end", type="string",
                      dest="end", help="report end date YYYY/MM/DD")
    parser.add_option("-d", "--dryrun", action="store_true", dest="is_test", default=False,
                      help="send emails only to _testers")

    options, arguments = parser.parse_args()
    Configuration.checkRequiredArguments(options, parser)
    return options, arguments


if __name__ == "__main__":
    opts, args = parse_opts()
    try:
        config = Configuration.Configuration()
        config.configure(opts.config)
        r = JobSuccessRateReporter(config, opts.start, opts.end, opts.vo, opts.template, opts.is_test, opts.verbose)
        r.generate()
        r.send_report()
    except:
        print >> sys.stderr, traceback.format_exc()
        sys.exit(1)
    sys.exit(0)
