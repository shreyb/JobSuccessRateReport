#!/usr/bin/python

import datetime


def dateparse(date):
    """Function to make sure that our date is either a list or a datetime.date object"""

    if isinstance(date,datetime.date):
        dateret=[date.year,date.month,date.day]
    elif isinstance(date,list):
        dateret = date
    else:
        raise TypeError("The date must be a datetime.date object or a list in the form of [yyyy,mm,dd]")
    return dateret



def indexpattern_generate(start,end):
    """Function to return the proper index pattern for queries to elasticsearch on gracc.opensciencegrid.org.  This improves performance by not just using a general index pattern unless absolutely necessary.
    This will especially help with reports, for example.i

    This function assumes that the date being passed in has been split into a list with [yyyy,mm,dd] format.  This gets tested.

    """ 
    
    startdate = dateparse(start)
    enddate = dateparse(end)
        
    
    basepattern='gracc.osg.raw-'
    
    if startdate[0]==enddate[0]:
        basepattern+=str(startdate[0])
        basepattern+='.'
    if startdate[1]==enddate[1]:
        if len(str(startdate[1]))==1:
            add = '0'+str(startdate[1])
        else:
            add = str(startdate[1])
        basepattern+=add
        basepattern+='.'
    
    basepattern+='*'
    return basepattern


if __name__=="__main__":
    #Meant for testing
    date_end = ['2016','06','12']
    date_start1 = ['2016','06','10']
    date_start2 = ['2016','05','10']
    date_start3 = ['2015','06','10']
   
    date_dateend = datetime.date(2016,06,12)
    date_datestart1 = datetime.date(2016,06,10)
    date_datestart2 = datetime.date(2016,5,10)
    date_datestart3 = datetime.date(2015,06,10)

    datebreak = '2016/06/10'

    print indexpattern_generate(date_start1,date_end)   #Should be gracc...2016.06.*
    print indexpattern_generate(date_start2,date_end)   #Should be gracc...2016.*
    print indexpattern_generate(date_start3,date_end)   #Should be gracc.osg.raw-*

    print indexpattern_generate(date_datestart1,date_dateend)
    print indexpattern_generate(date_datestart2,date_dateend)
    print indexpattern_generate(date_datestart3,date_dateend)

    print "This next test should fail with a TypeError."
    print indexpattern_generate(datebreak,date_dateend)



