#-------------------------------------------------------------------------------------
#
# utilities.py
#
# Various utility methods.
#
#-------------------------------------------------------------------------------------
import time, calendar, logging
from regexes import unsigned_number_regex
from constants import dateFormats, timeFormats

#-------------------------------------------------------------------------------------

def getBinLevel(cellcount, bin_levels):
    """
    Description:    Return the bin level category based on cellcount.
    Input:          cellcount:  <float> the organism cellcount.
                    bin_levels: <list>  List of Dictionaries containing bin level data.
    Return Value:   <string> the binlevel 'category'
    """
    
    for bin_level in bin_levels:
        if cellcount >= bin_level['MIN_VALUE'] and cellcount < bin_level['MAX_VALUE']:
            return bin_level['BIN_LEVEL']
    return 'not observed'

#-------------------------------------------------------------------------------------

def excel2epoch(val, datemode):
    """
    Description:    Convert Excel dates to Unix epoch.
    Input:          val:      <float>    The date as stored internally by Excel.
                    datemode: <integer>  The Excel workbook's datemode setting.
    Return Value:   <float> The date expressed as a Unix epoch timestamp.
    """

    import xlrd

    tt = list(xlrd.xldate_as_tuple(val, datemode))
    #
    # xldate_as_tuple() returns a 6-tuple, but we need a 9-tuple for mktime()
    #
    tt.extend([0,0,-1])
    return time.mktime(tt)

#-------------------------------------------------------------------------------------

def date2epoch(val):
    """
    Description:    Convert a date string to Unix epoch.
    Input:          val    <string>   The date string.
    Return Value:   <float>  The date expressed as a Unix epoch timestamp.
                    Will raise ValueError if the string won't parse.
    """

    retval = None
    for fmt in dateFormats:
       try:
           retval = calendar.timegm(time.strptime(val, fmt))
           break
       except ValueError:
           pass
    if retval is None:
        raise ValueError, 'Unrecognized date value %s'%val
    else:
        return retval
    
#-------------------------------------------------------------------------------------

def time2epoch(val):
    """
    Description:    Convert a time-of-day string to Unix epoch.
    Input:          val    <string>   The time string.
    Return Value:   <float>  The time of day expressed as a Unix epoch timestamp.
                    Will raise ValueError if the string won't parse.
    Discussion:     Typically the return value from this method will be added to the return value
                    of date2epoch(), so the return value should be relative to the epoch (usu. 1 Jan 1970 00:00:00 UTC).
                    BUT strptime(), when given only a time-of-day value, will return a tuple of the form
                    (1900, 1, 1, ...) for some damn reason.  So in this method we will first obtain the local
                    epoch with gmtime(0), and then manually load the parsed hour/min/sec values into the epoch tuple,
                    then convert to to seconds.  This will give us the time of day, in seconds, relative to the
                    epoch.
    """

    logging.debug('time2epoch()')
    #
    # Get the local epoch value
    #
    epoch = time.gmtime(0)
    for fmt in timeFormats:
       try:
           tt1 = time.strptime(val, fmt)
           logging.debug('tt1 %s'%`tt1`)
           #
           # Load up a second tuple with epoch values from gmtime() above, and
           # insert the parsed hour/minute/second values
           #
           tt2 = (epoch.tm_year, 
                  epoch.tm_mon, 
                  epoch.tm_mday,
                  tt1.tm_hour,
                  tt1.tm_min,
                  tt1.tm_sec,
                  epoch.tm_wday,
                  epoch.tm_yday,
                  epoch.tm_isdst)
           logging.debug('tt2 %s'%`tt2`)
           return calendar.timegm(tt2)
       except ValueError:
           pass
    #
    # If we get to this point, the input value didn't parse, so raise
    # a ValueError
    #
    raise ValueError, 'Unrecognized time of day value %s'%val

#-------------------------------------------------------------------------------------

def DMS2DD(val):
    """
    Description:    Convert a coordinate in degree/minute/second format to decimal degrees.
    Input:          val: <string> or <float>
                    Comes in several possible forms:
                      deg-min-sec, possibly with other formatting;
                      deg-min, with decimal minutes and possibly with other formatting;
                      unsigned decimal degrees.
    Return Value:   <unsigned float> unsigned decimal degrees.
    """
    try:
        retval = unsigned_number_regex.findall(val)
        try:
            deg, min, sec = retval
            retval = float(deg) + float(min)/60.0 + float(sec)/3600.0
        except ValueError:
            try:
               deg, min = retval
               retval = float(deg) + float(min)/60.0
            except ValueError:
               try:
                   retval = float(retval[0])
               except (ValueError, IndexError):
                   retval = 0.0
    except TypeError:
        retval = float(val)

    return retval
