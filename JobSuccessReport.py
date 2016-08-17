import sys
import os
import re
from datetime import datetime
import logging
from time import sleep

from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q

import traceback
import TextUtils
import Configuration
from Reporter import Reporter
from indexpattern import indexpattern_generate

class Jobs:
    def __init__(self):
        self.jobs = {}

    def add_job(self, site, job):
        if site not in self.jobs:
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

    def query(self, client):
        """Method that actually queries elasticsearch"""
        # Set up our search parameters
        # wildcardvoq = '*{}*'.format(self.config.get("query", "{}_voname".format(self.vo.lower())))
        voq = self.config.get("query", "{}_voname".format(self.vo.lower()))

        start_date = re.split('[-/ :]', self.start_time)
        starttimeq = datetime(*[int(elt) for elt in start_date]).isoformat()

        end_date = re.split('[-/ :]', self.end_time)
        endtimeq = datetime(*[int(elt) for elt in end_date]).isoformat()

        # Generate the index pattern based on the start and end dates
        indexpattern = indexpattern_generate(start_date, end_date)

        if self.verbose:
            print >> sys.stdout, indexpattern
            sleep(3)

        # Elasticsearch query
        resultset = Search(using=client, index = indexpattern) \
                .filter(Q({"term" : {"VOName" : voq}}))\
                .filter("range", EndTime={"gte" : starttimeq, "lt" : endtimeq})\
                .filter(Q({"term" : {"ResourceType" : "Payload"}}))
# .query("wildcard", VOName=wildcardvoq)\

        if self.verbose:
            print resultset.to_dict()

        return resultset

    def generate_result_array(self, resultset):
        # Compile results into array
        results = []
        for hit in resultset.scan():
            try:
                globaljobid = hit['GlobalJobId']
                realhost = re.sub('\s\(primary\)', '', hit['Host'])
                jobid = globaljobid.split('#')[1] + '@' + globaljobid[globaljobid.find('.') + 1:globaljobid.find('#')]
                outstr = '{starttime}\t{endtime}\t{JobID}\t{hostdescription}\t{host}\t{exitcode}'.format(
                                                     starttime = hit['StartTime'],
                                                     endtime = hit['EndTime'],
                                                     JobID = jobid,
                                                     hostdescription = hit['Host_description'],
                                                     host = realhost,
                                                     exitcode = hit['Resource_ExitCode']
                                                    )
                results.append(outstr)
                if self.verbose:
                    print >> sys.stdout, outstr
            except KeyError:
                pass # We want to ignore records where one of the above keys isn't listed in the ES document.  This is consistent with how the old MySQL report behaved.
        return results

    def add_to_clusters(self, results):
        # Grab each line in results, instantiate Job class for each one, and add to clusters
        for line in results:
            tmp = line.split('\t')
            start_time = tmp[0].strip().replace('T', ' ').replace('Z', '')
            end_time = tmp[1].strip().replace('T', ' ').replace('Z', '')
            jobid = tmp[2].strip()
            site = tmp[3].strip()
            if site == "NULL":
                continue
            host = tmp[4].strip()
            status = int(tmp[5].strip())
            job = Job(end_time, start_time, jobid, site, host, status)
            self.run.add_job(site, job)
            clusterid = jobid.split(".")[0]
            if clusterid not in self.clusters:
                self.clusters[clusterid] = []
            self.clusters[clusterid].append(job)
        return

    def generate(self):
        # Set up elasticsearch client
        client = Elasticsearch(['https://fifemon-es.fnal.gov'],
                             use_ssl = True,
                             verify_certs = True,
                             ca_certs = '/etc/grid-security/certificates/cilogon-osg.pem',
                             client_cert = 'gracc_cert/gracc-reports-dev.crt',
                             client_key = 'gracc_cert/gracc-reports-dev.key',
                             timeout = 60)

        resultset = self.query(client)              # Generate Search object for ES
        response = resultset.execute()              # Execute that Search
        return_code_success = response.success()	# True if the elasticsearch query completed without errors
        results = self.generate_result_array(resultset)  # Format our resultset into an array we use later

        if not return_code_success:
            raise Exception('Error accessing ElasticSearch')
        if len(results) == 1 and len(results[0].strip()) == 0:
            print >> sys.stdout, "Nothing to report"
            return

        self.add_to_clusters(results)               # Parse our results and create clusters objects for each
        return

    def send_report(self):
        table = ""
        total_jobs = 0
        total_failed = 0
        if len(self.run.jobs) == 0:
            return
        table_summary = ""
        job_table = ""

        # Look in clusters, figure out whether job failed or succeded, categorize appropriately, and generate HTML line for total jobs failed by cluster
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
            job_table += '\n<tr><td align = "left">{}</td><td align = "right">{}</td><td align = "right">{}</td><td></td><td></td><td></td><td></td><td></td><td></td></tr>'.format(cid,
                                                                                                                                                                                    total_jobs,
                                                                                                                                                                                    total_jobs_failed)
            # Generate HTML line for each failed job
            for job in failures:
                job_table += '\n<tr><td></td><td></td><td></td><td align = "left">{}</td><td align = "left">{}</td><td align = "left">{}</td><td align = "right">{}</td><td align = "right">{}</td><td align = "right">{}</td></tr>'.format(job.jobid,
                                                                                                                                                                                                                                            job.start_time,
                                                                                                                                                                                                                                            job.end_time,
                                                                                                                                                                                                                                            job.site,
                                                                                                                                                                                                                                            job.host,
                                                                                                                                                                                                                                            job.exit_code)

        total_jobs = 0

        # Compile count of failed jobs, calculate job success rate
        for key, jobs in self.run.jobs.items():
            failed = 0
            total = len(jobs)
            failures = {}
            for job in jobs:
                if job.exit_code != 0:
                    failed += 1
                    if job.host not in failures:
                        failures[job.host] = {}
                    if job.exit_code not in failures[job.host]:
                        failures[job.host][job.exit_code] = 0
                    failures[job.host][job.exit_code] += 1
            total_jobs += total
            total_failed += failed
            table_summary += '\n<tr><td align = "left">{}</td><td align = "right">{}</td><td align = "right">{}</td><td align = "right">{}</td></tr>'.format(key,
                                                                                                                                                             total,
                                                                                                                                                             failed,
                                                                                                                                                             round((total - failed) * 100. / total, 1))
            table += '\n<tr><td align = "left">{}</td><td align = "right">{}</td><td align = "right">{}</td><td align = "right">{}</td><td></td><td></td><td></td></tr>'.format(key,
                                                                                                                                                                                total,
                                                                                                                                                                                failed,
                                                                                                                                                                                round((total - failed) * 100. / total, 1))
            for host, errors in failures.items():
                for code, count in errors.items():
                    table += '\n<tr><td></td><td></td><td></td><td></td><td align = "left">{}</td><td align = "right">{}</td><td align = "right">{}</td></tr>'.format(host,
                                                                                                                                                                      code,
                                                                                                                                                                      count)

        table += '\n<tr><td align = "left">Total</td><td align = "right">{}</td><td align = "right">{}</td><td align = "right">{}</td><td></td><td></td><td></td></tr>'.format(total_jobs,
                                                                                                                                                                               total_failed,
                                                                                                                                                                               round((total_jobs - total_failed) * 100. / total_jobs, 1))
        table_summary += '\n<tr><td align = "left">Total</td><td align = "right">{}</td><td align = "right">{}</td><td align = "right">{}</td></td></tr>'.format(total_jobs,
                                                                                                                                                                 total_failed,
                                                                                                                                                                 round((total_jobs - total_failed) * 100. / total_jobs, 1))
        # Grab HTML template, replace variables shown
        text = "".join(open(self.template).readlines())
        text = text.replace("$START", self.start_time)
        text = text.replace("$END", self.end_time)
        text = text.replace("$TABLE_SUMMARY", table_summary)
        text = text.replace("$TABLE_JOBS", job_table)
        text = text.replace("$TABLE", table)
        text = text.replace("$VO", self.vo)

        # Generate HTML file to send
        fn = "{}-jobrate.{}".format(self.vo.lower(),
                                    self.start_time.replace("/", "-"))

        with open(fn, 'w') as f:
            f.write(text)

        # The part that actually emails people.
        if self.is_test:
            emails = re.split('[; ,]', self.config.get("email", "test_to"))
        else:
            emails = re.split('[; ,]', self.config.get("email", "{}_email".format(self.vo.lower()))) + \
                     re.split('[: ,]', self.config.get("email", "test_to"))

        TextUtils.sendEmail(([], emails),
                                "{} Production Jobs Success Rate on the OSG Sites ({} - {})".format(self.vo,
                                                                                                self.start_time,
                                                                                                self.end_time),
                                {"html": text},
                                ("Gratia Operation", "sbhat@fnal.gov"),
                                "smtp.fnal.gov")

        os.unlink(fn)       # Delete HTML file


if __name__ == "__main__":
    opts, args = Reporter.parse_opts()

    if opts.debug:
        logging.basicConfig(filename='jobsuccessreport.log', level=logging.DEBUG)
    else:
        logging.basicConfig(filename='jobsuccessreport.log', level=logging.ERROR)
        logging.getLogger('elasticsearch.trace').addHandler(logging.StreamHandler())

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
