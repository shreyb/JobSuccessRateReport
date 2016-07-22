#!/bin/python

from datetime import datetime, timedelta
import threading
import Queue
import time
import shlex
import subprocess

from JobSuccessReport import *

#Initial variables
VOs = ('UBooNE', 'NOvA', 'DUNE', 'Mu2e')
yesterday = datetime.strftime(datetime.today()-timedelta(days=1),'%Y-%m-%d %H:%M:%S')
today = datetime.strftime(datetime.today(),'%Y-%m-%d %H:%M:%S')


def somework(vo):
    command = ['python', 'JobSuccessReport.py', '-c', 'jobrate.config', '-E', vo, '-s', yesterday, '-e', today, '-T', 'template_jobrate.html', '-d'] 
    subprocess.call(command)
    print "Work is done in thread {} for VO {}".format(threading.current_thread().name, vo)
    return

def threader(q):
    worker = q.get()
    somework(worker)
    q.task_done()

def main():
    q = Queue.Queue()

    for i in range(8):
        t = threading.Thread(target = threader, name = 'Thread {}'.format(i), args=(q,))
        t.daemon = True
        t.start()

    for vo in VOs:
        print vo, yesterday, today
        q.put(vo)
        print "{} added to the queue".format(vo)
        
    q.join()

#    opts, args = parse_opts()
#    
#    if opts.debug:
#    logging.basicConfig(filename='jobsuccessreport.log',level=logging.DEBUG)
#    else:
#    logging.basicConfig(filename='jobsuccessreport.log',level=logging.ERROR)
#    logging.getLogger('elasticsearch.trace').addHandler(logging.StreamHandler())
#    
#    try:
#        config = Configuration.Configuration()
#        config.configure(opts.config)
#        r = JobSuccessRateReporter(config, opts.start, opts.end, opts.vo, opts.template, opts.is_test, opts.verbose)
#        r.generate()
#        r.send_report()
#    except:
#        print >> sys.stderr, traceback.format_exc()
#        sys.exit(1)
#    sys.exit(0)



if __name__ == "__main__":
    main()





 

