import sys
import os
import re
from datetime import datetime
import logging
from time import sleep

from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q
import certifi

import optparse
import traceback
import TextUtils
import Configuration
from Reporter import Reporter
from indexpattern import indexpattern_generate

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
        self.title = "Production Jobs Success Rate {0} - {1}".format(self.start_time, self.end_time)
        self.run = Jobs()
        self.clusters = {}
        self.connectStr = None

    def generate(self):
        logging.basicConfig(filename='example.log',level=logging.ERROR)
        logging.getLogger('elasticsearch.trace').addHandler(logging.StreamHandler())

        client=Elasticsearch(['https://fifemon-es.fnal.gov'],
                             use_ssl = True,
                             verify_certs = True,
                             ca_certs = certifi.where(),
            			     client_cert = 'gracc_cert/gracc-reports-dev.crt',
                             client_key = 'gracc_cert/gracc-reports-dev.key',
                             timeout = 60)
        

        wildcardcommonnameq = '*{}*'.format(self.config.get("query", "{}_commonname".format(self.vo.lower())))
        wildcardvoq = '*{}*'.format(self.vo.lower())
        
        start_date = re.split('[/ :]', self.start_time)
        starttimeq = datetime(*[int(elt) for elt in start_date]).isoformat()
        
        end_date = re.split('[/ :]', self.end_time)
        endtimeq = datetime(*[int(elt) for elt in end_date]).isoformat()
        
        indexpattern=indexpattern_generate(start_date,end_date)
        
        if self.verbose:
            print >> sys.stdout,indexpattern
            sleep(3)

        resultset = Search(using=client,index=indexpattern) \
                .query("wildcard",VOName=wildcardvoq)\
                .query("wildcard",CommonName=wildcardcommonnameq)\
                .filter("range",EndTime={"gte":starttimeq,"lt":endtimeq})\
                .filter(Q({"term":{"ResourceType":"Payload"}}))	
        
        querystringverbose=resultset.to_dict()	

        response = resultset.execute()
        return_code_success = response.success()	# True if the elasticsearch query completed without errors
        
        results=[]
        for hit in resultset.scan():
            try:
                globaljobid = hit['GlobalJobId']
                jobid = globaljobid.split('#')[1]+'@'+globaljobid[globaljobid.find('.')+1:globaljobid.find('#')]
                outstr = '{starttime}\t{endtime}\t{JobID}\t{hostdescription}\t{host}\t{exitcode}'.format(
                                                     starttime = hit['StartTime'],
                                                     endtime = hit['EndTime'],
                                                     JobID = jobid,
                                                     hostdescription = hit['Host_description'],
                                                     host = hit['Host'],
                                                     exitcode = hit['Resource_ExitCode']
                                                    )
                results.append(outstr)
                if self.verbose:
                    print >> sys.stdout, outstr
            except KeyError as e:
                pass # We want to ignore records where one of the above keys isn't listed in the ES document.  This is consistent with how the old MySQL report behaved. 
        
        if self.verbose:
            print >> sys.stdout, querystringverbose
        if not return_code_success:
            raise Exception('Error accessing ElasticSearch')
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
			    ("Gratia Operation", "tlevshin@fnal.gov"), "smtp.fnal.gov")

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
