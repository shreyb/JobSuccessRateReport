#!/usr/bin/python


def indexpattern_generate(start,end):
    """Function to return the proper index pattern for queries to elasticsearch on gracc.opensciencegrid.org.  This improves performance by not just using a general index pattern unless absolutely necessary.
    This will especially help with reports, for example.i

    This function assumes that the date being passed in has been split into a list with [yyyy,mm,dd] format.

    """ 
    basepattern='gracc.osg.raw-'
    
    if start[0]==end[0]:
        basepattern+=str(start[0])
        basepattern+='.'
    if start[1]==end[1]:
        basepattern+=str(start[1])
        basepattern+='.'
    
    basepattern+='*'
    return basepattern


if __name__=="__main__":
    #Meant for testing
    date_end = ['2016','06','12']
    date_start1 = ['2016','06','10']
    date_start2 = ['2016','05','10']
    date_start3 = ['2015','06','10']
    

    print indexpattern_generate(date_start1,date_end)   #Should be gracc...2016.06.*
    print indexpattern_generate(date_start2,date_end)   #Should be gracc...2016.*
    print indexpattern_generate(date_start3,date_end)   #Should be gracc.osg.raw-*



