#---------------------------------------------------------------------------------------------
#
# textProc.py
#	Routine(s) to process text files containing HABS data.
#
#---------------------------------------------------------------------------------------------

import time, logging, pprint, csv, _csv, os, socket
import codecs
from constants import *
from QA import *
from utilities import *
from validation import checkRange
from ranges import validRanges
from EcoRegionChecker import findEcoRegionForCoords
from exception import InfobrokerError

#---------------------------------------------------------------------------------------------

def processTextFL(fileName, organisms, bin_levels, options, config, infoBroker):
    """
Description:    Process Florida HABS data from tab-delimited text files.
Input:          fileName:   <string>    Name of the text file.
                organisms:  <list>      List of ORGANISM records from the database
                bin_levels: <list>      List of BIN_LEVEL records from the database
                options:    <obj>       OptionParser (command-line options) object.
                config:     <obj>       ConfigParser application configuration data.
                infoBroker: <obj>       XMLRPC ServerProxy object.
Output:         None.
Return Value:   This method is a generator.  For each valid data line it returns a Tuple (rowdata, qadata, qacomments).
    """

    logging.info('processTextFL():  %s'%fileName)
    state = 'FL'
    #
    # We have an issue with the data managers occasionally misnaming a file or otherwise
    # uploading data files which we are not expecting, causing this processor to crash.
    # We use this list o' terms in an attempt to verify that we're looking at the right file.
    #
    requiredHeaderTerms = ['LATITUDE','LONGITUDE','DATE','TIME','LOCATION','HABSOS_ID','K_brevis']
    try:
        #
        # Open the input file and determine its dialect (CSV or TSV),
        # then rewind and instantiate a csv.reader
        #
        datafp = open(fileName, 'r')
        dialect = csv.Sniffer().sniff(datafp.readline())
        datafp.seek(0)
        reader = csv.reader(datafp, dialect=dialect)
        #
        # Extract the column names from the first line in the file and map them
        # to our own vocabulary terms.
        #
        headers = [x.strip() for x in reader.next()]
        for ndx in xrange(len(headers)):
            try:
                headers[ndx] = termMap[state][headers[ndx]]
            except KeyError:
                pass
        #
        # Header check:  make sure we have all required terms in the header 
        #
        logging.info('Performing header check')
        goAhead = True
        for requiredHeaderTerm in requiredHeaderTerms:
            if requiredHeaderTerm not in headers:
                logging.warning('  %s not found--bailing out'%`requiredHeaderTerm`)
                goAhead = False
                break
                
        if goAhead:
            logging.info('Header check OK')
            if not options.dry_run:
                #
                # Get a list of HABSOS sample IDs from the database.  Because the text files from Florida
                # contain a year's worth of data in a sliding window, we want to be sure to only process
                # samples which haven't already been processed.  We ensure this by filtering out samples
                # which contain IDs that are already in the database.
                #
                logging.info('Getting HABSOS IDs from the database')
                try:
                    habsosIds = infoBroker.querySDE('habsos','samples','habsos_id')
                    logging.info('   %d IDs returned'%len(habsosIds))
                    habsosIds = sorted(list(set([x['HABSOS_ID'] for x in habsosIds])), reverse=True)
                except (TypeError, KeyError), msg:
                    logging.warning('Error encountered getting HABSOS IDs: %s'%str(msg))
                    logging.info('   Return value from query was %s'%`habsosIds`)
                    if 'Error' in habsosIds:
                        raise InfobrokerError, habsosIds['Error']
                    else:
                        raise InfobrokerError, str(msg)
                except socket.error, msg:
                    raise InfobrokerError, str(msg)
            else:
                habsosIds = []
            #
            # 'Unproofed' ids are those records which were inserted into the database without a proof date.
            # Those IDs were written into a text file; we check these to see if one of these records was
            # subsequently proofed, thereby possibly requiring an update to the database.
            #
            #proofFileName = os.path.join(config.get('globals','dataFileDir'), 'unproofed.txt')
            #try:
            #    unproofedIds = sorted(open(proofFileName,'r').read().split('\n'), reverse=True)
            #except IOError:
            #    unproofedIds = []
            #
            # Setup complete; process the file line-by-line
            # 
            for line in reader:
                vals = [x.strip() for x in line]
                #
                # 20140130  Perform an additional sanity check on incoming data:  do the number of tab-delimited
                #           values in the data line equal the number of header values?
                #           If not, bail and move on to the next line.
                #
                if len(vals) == len(headers):
                    rowdata = dict(zip(headers, vals))
                    #
                    # Only process records that were previously unproofed as well as those
                    # whose IDs are not already in the database.
                    #
                    if rowdata['HABSOS_ID'] not in habsosIds:
                    #if rowdata['HABSOS_ID'] in unproofedIds or rowdata['HABSOS_ID'] not in habsosIds:
                        #
                        # Sometimes there's extended-ASCII characters in the data...
                        # replace with a blank.
                        #
                        for key in rowdata:
                           newVal = rowdata[key]
                           for c in newVal:
                              if ord(c) > 127:
                                 newVal = newVal.replace(c, ' ')
                           rowdata[key] = newVal.strip()
    
                        logging.debug('Rowdata: %s'%pprint.pformat(rowdata))
                        #
                        # Default database operation is 'insert'
                        #
                        rowdata['OPERATION'] = 'insert'
                        try:
                            #
                            # Check for proof date.  If none, go ahead and insert it but keep the ID
                            # to check against subsequent reports.  If we have a proof date and the ID
                            # is in our list of unproofed IDs, we will perform an update.  If the ID
                            # is not in our list of unproofed IDs, it's a straight-up insert.
                            #
                            #if not rowdata['PROOF_DATE']:
                            #    #
                            #    # If this ID is already in unproofedIds, ignore 'cause we don't want to insert it
                            #    # again
                            #    #
                            #    if rowdata['HABSOS_ID'] in unproofedIds:
                            #        continue
                            #    else:
                            #        #
                            #        # Add this ID to the list of unproofed IDs, and continue with the ingest.
                            #        #
                            #        unproofedIds.append(rowdata['HABSOS_ID'])
                            #else:
                            #    try:
                            #        unproofedIds.remove(rowdata['HABSOS_ID'])
                            #        rowdata['OPERATION'] = 'update'
                            #    except ValueError:
                            #        pass
         
                            rowdata['LATITUDE'] = float(rowdata['LATITUDE'])
                            rowdata['LONGITUDE'] = float(rowdata['LONGITUDE'])
                            rowdata['STATE'] = state
                            if checkRange(rowdata['LATITUDE'], validRanges['LATITUDE']): 
                                if checkRange(rowdata['LONGITUDE'], validRanges['LONGITUDE']):
                                    rowdata.update({'AGENCY':'FWC', 'CELLCOUNTS':{}, 'ORGANISM_IDS': {}, 'CATEGORIES': {}})
                                    #
                                    # Term collison here:  FL text file has a 'DESCRIPTION' column, but we
                                    # use 'DESCRIPTION' in the database to hold the sampling site name.
                                    # So we do an explicit swap here.
                                    #
                                    rowdata['DESCRIPTION'] = rowdata.pop('LOCATION')
                                    logging.info('Original date/time: %s %s'%(`rowdata['DATE']`,`rowdata['TIME']`))
                                   
                                    try:
                                        rowdata['DATE'] = time.mktime(time.strptime('%s %s'%(rowdata['DATE'],rowdata['TIME']), '%m/%d/%Y %H:%M'))
                                    except ValueError:
                                        rowdata['DATE'] = time.mktime(time.strptime(rowdata['DATE'], '%m/%d/%Y'))
                                        rowdata['QA_COMMENT'] = 'Missing/invalid time of day'
                                    rowdata.pop('TIME')           
                                    #
                                    # Hacktastic:  time of day in the input is local, without a timezone hint.  We want to store
                                    # time as UTC, so use the longitude to determine the timezone offset.  85.4194W is the westernmost
                                    # longitude on the Florida panhandle for the Eastern time zone.  Anything west of that in our
                                    # input will be presumed to be in the Central time zone.
                                    #
                                    # First run the timestamp through time.localtime() to see if DST applies.
                                    #
                                    dst = time.localtime(rowdata['DATE'])[8]
                                    #
                                    # Check longitude and determine the (non-DST) UTC offset.
                                    # In the western hemisphere, UTC offsets are negative.
                                    #
                                    if rowdata['LONGITUDE'] <= -85.4194:
                                        logging.info('Timezone based on longitude (%f): CENTRAL'%rowdata['LONGITUDE'])
                                        tzoffset = -6*3600
                                    else:
                                        logging.info('Timezone based on longitude (%f): EASTERN'%rowdata['LONGITUDE'])
                                        tzoffset = -5*3600
                                    #
                                    # Adjust for DST.
                                    #
                                    tzoffset += dst*3600
                                    #
                                    # Finally, subtract the UTC offset from the local time.
                                    #
                                    rowdata['DATE'] -= tzoffset
                                    logging.info('UTC corrected date/time: %s'%time.strftime('%m/%d/%Y %H:%M',time.localtime(rowdata['DATE'])))
                                    #
                                    # Convert cell counts to integers; categorize.
                                    #
                                    try:
                                        rowdata['CELLCOUNTS'].update({'Karenia brevis':int(rowdata['K_brevis'])})
    
                                        for organismName in rowdata['CELLCOUNTS']:
                                            rowdata['ORGANISM_IDS'][organismName] = [o['OBJECTID'] for o in organisms if o['ORGANISM_NAME'] == organismName][0]
                                            #
                                            # Bin level (category)
                                            #
                                            rowdata['CATEGORIES'][organismName] =  getBinLevel(rowdata['CELLCOUNTS'][organismName], bin_levels[organismName])
    
                                        qadata, qacomments = initQAVariables([x for x in rowdata['CELLCOUNTS']])
                                        #
                                        # Determine ecosystem/ecoregion
                                        #
                                        regions = findEcoRegionForCoords(rowdata['LONGITUDE'],rowdata['LATITUDE'])
                                        try:
                                            rowdata['ECOSYSTEM'] = regions[0][0].title()
                                        except IndexError:
                                            pass
                                        try:
                                            rowdata['ECOREGION'] = regions[1][0].title()
                                        except IndexError:
                                            pass
                                        #
                                        # Remove items with empty string values or not in termMap.
                                        #
                                        for key in rowdata.keys():
                                            if key not in termMap[state].values():
                                                logging.debug('  Popping non-termMap key %s'%key)
                                                rowdata.pop(key)
                                            elif rowdata[key] == '':
                                                logging.debug('  Popping empty-value key %s'%key)
                                                rowdata.pop(key)
                                        #
                                        # QA the data
                                        #
                                        doQA(rowdata, qadata, qacomments)
                                        yield rowdata, qadata, qacomments
                                    except ValueError:
                                        logging.warning('%s CELLCOUNTS missing or invalid: %s -- record discarded'%(rowdata['HABSOS_ID'], `rowdata['CELLCOUNTS']`))
                                        logging.info('----------------------------------------')
                                else:
                                    logging.warning('%s Longitude out of range: %f (valid range %.1f - %.1f) -- record discarded'%(rowdata['HABSOS_ID'],
                                                                                                                                   rowdata['LONGITUDE'],
                                                                                                                                   validRanges['LONGITUDE'][0],
                                                                                                                                   validRanges['LONGITUDE'][1]))
                                    logging.info('----------------------------------------')
    
                            else:
                                logging.warning('%s Latitude out of range: %f (valid range %.1f - %.1f) -- record discarded'%(rowdata['HABSOS_ID'],
                                                                                                                              rowdata['LATITUDE'],
                                                                                                                              validRanges['LATITUDE'][0],
                                                                                                                              validRanges['LATITUDE'][1]))
                                logging.info('----------------------------------------')
                        except KeyError, msg:
                            logging.warning('KeyError: %s; not processed.'%str(msg))
                            logging.warning(' Line was %s\n'%'\t'.join(line))
                        except ValueError, msg:
                            logging.warning('ValueError: %s; not processed.'%str(msg))
                            logging.warning(' Line was %s\n'%'\t'.join(line))
                    #
                    # Store any remaining unproofed ids
                    #
                    #unproofedIds = sorted(list(set(unproofedIds)))
                    #if not options.dry_run:
                    #    try:
                    #        fp = open(proofFileName,'w')
                    #        fp.write('\n'.join(unproofedIds))
                    #        fp.close() 
                    #    except IOError, msg:
                    #        logging.warning('Unable to open %s for writing: %s'%(proofFileName, str(msg)))
                else:
                    logging.warning('Record at line %d discarded: header/value mismatch'%ndx)
                    logging.info('  Data: %s'%'\t'.join(line))
            #
            # Close the input file.
            #
            datafp.close()
    except IOError, msg:
        logging.warning('processTextFL(): IOError on file %s: %s'%(fileName, str(msg)))
    except (csv.Error, _csv.Error), msg:
        logging.warning('processTextFL(): csv error on file %s: %s'%(fileName, str(msg)))

#---------------------------------------------------------------------------------------------

def processTextTX(fileName, organisms, bin_levels, options, config, infoBroker):
    """
Description:    Process Texas HABS data from tab-delimited text files.
Input:          fileName:   <string>    Name of the text file.
                organisms:  <list>      List of ORGANISM records from the database
                bin_levels: <list>      List of BIN_LEVEL records from the database
                options:    <obj>       OptionParser (command-line options) object.
                config:     <obj>       ConfigParser application configuration data.
                infoBroker: <obj>       XMLRPC ServerProxy object.
Output:         None.
Return Value:   This method is a generator.  For each valid data line it returns a Tuple (rowdata, qadata, qacomments).
    """

    state = 'TX'
    #
    # Get the current time.
    #
    now = time.time()
    #
    # 
    #
    try:
        fp = open(fileName, 'r')
        dialect = csv.Sniffer().sniff(fp.readline())
        fp.seek(0)
        reader = csv.reader(fp, dialect=dialect)

        if options.startingRow < 1:
            options.startingRow = 1;
        #
        # Process the remaining rows in worksheet, beginning with 'startingRow'
        #
        logging.info('Getting headers...')
        #
        # Read column headers and map them to database terms
        #   ASSUME row 0 contains the column headers
        #
        headers = [x.strip() for x in reader.next()]
        for ndx in xrange(len(headers)):
           try:
               headers[ndx] = termMap[state][headers[ndx]]
           except KeyError:
               pass
        #
        # Advance the file pointer to 'startingRow'
        #
        for ctr in xrange(options.startingRow-1):
            reader.next()

        ctr = options.startingRow - 1
        for line in reader:
            ctr += 1
            logging.info('Row %d'%(ctr))
            rowdata = {'STATE':state, 'CELLCOUNTS':{}, 'ORGANISM_IDS': {}, 'CATEGORIES':{}}
            #
            # So far these files report only K. brevis counts, but we will need to support multiple
            # species in future.  We behave as if multiple species are possible in these data.
            # Also K. brevis is not explicitly indicated in the data; we ASSUME it here.
            #
            genus = 'Karenia'
            species = 'brevis'
            organismName = '%s %s'%(genus, species)
            #
            # Get the database ID for this organism
            #
            for organism in organisms:
                if organism['ORGANISM_NAME'] == organismName:
                    rowdata['ORGANISM_IDS'][organismName] = organism['OBJECTID']
                    break
            #
            # Accumulate data values in a list, and build a dictionary using the headers & vals
            #
            vals = [x.strip() for x in line]
            #
            # Convert Unicode to ASCII; ignore any 'special' chars (I hope)
            #
            for ndx in xrange(len(vals)):
               if type(vals[ndx]) == types.UnicodeType:
                   vals[ndx] = codecs.decode(vals[ndx].strip(), 'ascii', 'replace')
            rowdata.update(dict(zip(headers, vals)))
            logging.debug('Raw data: %s'%pprint.pformat(rowdata))
            #
            # Initialize all QA variables to QA_MISSING and all comments to empty strings
            #
            qadata, qacomments = initQAVariables([organismName])
            dateTime = ('%s %s'%(rowdata['DATE'], rowdata['TIME'])).strip()
            logging.debug('Incoming datetime %s'%`dateTime`)
            rowdata['DATE'] = None
            for fmt in dateFormats:
                try:
                    rowdata['DATE'] = time.mktime(time.strptime(dateTime, fmt))
                    logging.debug('Successfully parsed datetime')
                    #
                    # Is the sample time in the future?  This can happen if the column or cell is misformatted; e.g., the observer
                    # entered '0900' vice '9:00' and/or the column wasn't formatted for time values. 
                    #
                    if rowdata['DATE'] > now:
                        logging.warning('***Sample time is in the future!  Incoming time value was %s. -- ignored'%`rowdata['TIME']`)
                        continue
                    rowdata.pop('TIME')
                    #
                    # Hacktastic:  time of day in the input is local, without a timezone hint.  We want to store
                    # time as UTC.
                    #
                    # First run the timestamp through time.localtime() to see if DST applies.
                    #
                    dst = time.localtime(rowdata['DATE'])[8]
                    #
                    # Texas is entirely in the Central timezone (-6 hours from UTC).
                    #
                    tzoffset = -6*3600 + dst*3600
                    rowdata['DATE'] -= tzoffset
                    break
                except ValueError:
                    pass 
            #
            # Latitude
            #
            rowdata['LATITUDE'] = DMS2DD(rowdata['LATITUDE'])
            #
            # Longitude
            #
            rowdata['LONGITUDE'] = DMS2DD(rowdata['LONGITUDE'])
            #
            # Longitudes are all West longitudes (so far) but are not signed in some cases.  Flip the sign
            # if necessary...
            #
            if rowdata['LONGITUDE'] > 0.0:
                rowdata['LONGITUDE'] = -rowdata['LONGITUDE']
            #
            # Cellcount, by organism.
            #    Sometimes contains text like 'inconclusive' or 'TNTC' (Too Numerous To Count)
            #
            rowdata['CELLCOUNTS'][organismName] = float(rowdata['CELLCOUNT'])
            #
            # Must have a date and a position to continue
            #
            logging.debug('Parsed data: %s'%pprint.pformat(rowdata))
            if rowdata['DATE'] and rowdata['LATITUDE'] and rowdata['LONGITUDE']:
                if checkRange(rowdata['LATITUDE'], validRanges['LATITUDE']):
                    if checkRange(rowdata['LONGITUDE'], validRanges['LONGITUDE']):
                        #
                        # Wind direction (compass heading) to azimuth
                        # We have a choice of wind direction columns: choose WDIR1 if available;
                        # otherwise use WDIR2
                        #
                        try:
                            rowdata['WDIR'] = rowdata['WDIR1'] or rowdata['WDIR2']
                            rowdata.pop('WDIR1')
                            rowdata.pop('WDIR2')
                        except KeyError:
                            try:
                                rowdata['WDIR'] = rowdata['WDIR1']
                                rowdata.pop('WDIR1')
                            except KeyError:
                                try:
                                    rowdata['WDIR'] = rowdata['WDIR2']
                                    rowdata.pop('WDIR2')
                                except KeyError:
                                    pass
                        #
                        # Wind speed
                        # We have a choice of wind speed columns: choose WSPD1 if available;
                        # otherwise use WSPD2
                        #
                        try:
                            rowdata['WSPD'] = rowdata['WSPD1'] or rowdata['WSPD2']
                            rowdata.pop('WSPD1')
                            rowdata.pop('WSPD2')
                        except KeyError:
                            try:
                                rowdata['WSPD'] = rowdata['WSPD1']
                                rowdata.pop('WSPD1')
                            except KeyError:
                                try:
                                    rowdata['WSPD'] = rowdata['WSPD2']
                                    rowdata.pop('WSPD2')
                                except KeyError:
                                    pass
                        #
                        # Water depth
                        #
                        try:
                            rowdata['DEPTH'] = float(rowdata['DEPTH']) * F2M
                            qadata['DEPTH'] = QA_CHANGED
                            qacomments['DEPTH'] = 'Converted from feet to meters'
                        except (TypeError,ValueError):
                            if rowdata['DEPTH'].lower() == 'surface':
                                rowdata['DEPTH'] = 0.0
                                qadata['DEPTH'] = QA_CHANGED
                                qacomments['DEPTH'] = 'Converted from "surface" to 0.0'
                            else:
                                rowdata.pop('DEPTH')
                        #
                        # Determine ecosystem/ecoregion 
                        #
                        regions = findEcoRegionForCoords(rowdata['LONGITUDE'],rowdata['LATITUDE'])
                        try:
                            rowdata['ECOSYSTEM'] = regions[0][0].title()
                        except IndexError:
                            pass
                        try:
                            rowdata['ECOREGION'] = regions[1][0].title()
                        except IndexError:
                            pass
                        #
                        # QA the data
                        #
                        doQA(rowdata, qadata, qacomments)
                        #
                        # Remove items with empty string values or not in termMap.
                        #
                        for key in rowdata.keys():
                            if rowdata[key] == '':
                                rowdata.pop(key)

                        for key in rowdata.keys():
                            if key not in termMap[state].values():
                               rowdata.pop(key)
                        #
                        # Bin level (category)
                        #
                        # 20140806  David E. Sallis
                        #   TX reports cellcounts in cells/mL; database units are cells/L.  So multiply TX cellcounts by 1000.0.
                        #
                        rowdata['CELLCOUNTS'][organismName] *= 1000.0
                        rowdata['CATEGORIES'][organismName] =  getBinLevel(rowdata['CELLCOUNTS'][organismName], bin_levels[organismName])
                        qadata['CELLCOUNTS'][organismName] = QA_CHANGED
                        qacomments['CELLCOUNTS'][organismName] = "Extrapolated from original cells/mL value"

                        yield rowdata, qadata, qacomments
                    else:
                        logging.warning('Longitude out of range: %f (valid range %f - %f) -- record discarded'%(rowdata['LONGITUDE'],
                                                                                                                validRanges['LONGITUDE'][0],
                                                                                                                validRanges['LONGITUDE'][1]))
                else:
                    logging.warning('Latitude out of range: %f (valid range %f - %f) -- record discarded'%(rowdata['LATITUDE'],
                                                                                                           validRanges['LATITUDE'][0],
                                                                                                           validRanges['LATITUDE'][1]))
            else:
                logging.warning('***Row %d:  Date and/or location missing -- ignored'%(ctr))
    except IOError, msg:
        logging.warning('IOError encountered.  %s'%str(msg))

#---------------------------------------------------------------------------------------------

def processTextAL(fileName, organisms, bin_levels, options, config, infoBroker):
    """
Description:    Process Alabama HABS data from tab-delimited text files.
Input:          fileName:   <string>    Name of the text file.
                organisms:  <list>      List of ORGANISM records from the database
                bin_levels: <list>      List of BIN_LEVEL records from the database
                options:    <obj>       OptionParser (command-line options) object.
                config:     <obj>       ConfigParser application configuration data.
                infoBroker: <obj>       XMLRPC ServerProxy object.
Output:         None.
Return Value:   This method is a generator.  For each valid data line it returns a Tuple (rowdata, qadata, qacomments).
    """

    state = 'AL'
    #
    # Get the current time.
    #
    now = time.time()
    #
    # 
    #
    try:
        fp = open(fileName, 'r')
        dialect = csv.Sniffer().sniff(fp.readline())
        fp.seek(0)
        reader = csv.reader(fp, dialect=dialect)

        if options.startingRow < 1:
            options.startingRow = 1;
        #
        # Process the remaining rows in worksheet, beginning with 'startingRow'
        #
        logging.info('Getting headers...')
        #
        # Read column headers and map them to database terms
        #   ASSUME row 0 contains the column headers
        #
        headers = [x.strip() for x in reader.next()]
        for ndx in xrange(len(headers)):
           try:
               headers[ndx] = termMap[state][headers[ndx]]
           except KeyError:
               pass
        #
        # Advance the file pointer to 'startingRow'
        #
        for ctr in xrange(options.startingRow-1):
            reader.next()

        ctr = options.startingRow - 1
        for line in reader:
            ctr += 1
            logging.info('Row %d'%(ctr))
            rowdata = {'STATE':state, 'CELLCOUNTS':{}, 'ORGANISM_IDS': {}, 'CATEGORIES':{}}
            #
            # So far these files report only K. brevis counts, but we will need to support multiple
            # species in future.  We behave as if multiple species are possible in these data.
            # Also K. brevis is not explicitly indicated in the data; we ASSUME it here.
            #
            genus = 'Karenia'
            species = 'brevis'
            organismName = '%s %s'%(genus, species)
            #
            # Get the database ID for this organism
            #
            for organism in organisms:
                if organism['ORGANISM_NAME'] == organismName:
                    rowdata['ORGANISM_IDS'][organismName] = organism['OBJECTID']
                    break
            #
            # Accumulate data values in a list, and build a dictionary using the headers & vals
            #
            vals = [x.strip() for x in line]
            if len(vals) == len(headers):
                #
                # Convert Unicode to ASCII; ignore any 'special' chars (I hope)
                #
                for ndx in xrange(len(vals)):
                   if type(vals[ndx]) == types.UnicodeType:
                       vals[ndx] = codecs.decode(vals[ndx].strip(), 'ascii', 'replace')
                rowdata.update(dict(zip(headers, vals)))
                logging.debug('Raw data: %s'%pprint.pformat(rowdata))
                #
                # Initialize all QA variables to QA_MISSING and all comments to empty strings
                #
                qadata, qacomments = initQAVariables([organismName])
                dateTime = '%s %s'%(rowdata['DATE'],rowdata['TIME'])
                logging.debug('Incoming datetime %s'%`dateTime`)
                rowdata['DATE'] = None
                for fmt in dateFormats:
                    try:
                        rowdata['DATE'] = time.mktime(time.strptime(dateTime, fmt))
                        logging.debug('Successfully parsed datetime')
                        #
                        # Is the sample time in the future?  This can happen if the column or cell is misformatted; e.g., the observer
                        # entered '0900' vice '9:00' and/or the column wasn't formatted for time values. 
                        #
                        if rowdata['DATE'] > now:
                            logging.warning('***Sample time is in the future!  Incoming time value was %s. -- ignored'%`rowdata['TIME']`)
                            continue
                        rowdata.pop('TIME')
                        #
                        # Hacktastic:  time of day in the input is local, without a timezone hint.  We want to store
                        # time as UTC.
                        #
                        # First run the timestamp through time.localtime() to see if DST applies.
                        #
                        dst = time.localtime(rowdata['DATE'])[8]
                        #
                        # Alabama is entirely in the Central timezone (-6 hours from UTC).
                        #
                        tzoffset = -6*3600 + dst*3600
                        rowdata['DATE'] -= tzoffset
                        break
                    except ValueError:
                        pass 
                #
                # Latitude
                #
                rowdata['LATITUDE'] = DMS2DD(rowdata['LATITUDE'])
                #
                # Longitude
                #
                rowdata['LONGITUDE'] = DMS2DD(rowdata['LONGITUDE'])
                #
                # Longitudes are all West longitudes (so far) but are not signed in some cases.  Flip the sign
                # if necessary...
                #
                if rowdata['LONGITUDE'] > 0.0:
                    rowdata['LONGITUDE'] = -rowdata['LONGITUDE']
                #
                # Cellcount, by organism.
                #    Sometimes contains text like 'inconclusive' or 'TNTC' (Too Numerous To Count)
                #
                try:
                    rowdata['CELLCOUNTS'][organismName] = float(rowdata['CELLCOUNT'].replace(',',''))
                except ValueError, msg:
                    logging.warning('ValueError: CELLCOUNTS %s'%str(msg))
                    rowdata['CELLCOUNTS'][organismName] = 0.0
                #
                # Must have a date and a position to continue
                #
                logging.debug('Parsed data: %s'%pprint.pformat(rowdata))
                if rowdata['DATE'] and rowdata['LATITUDE'] and rowdata['LONGITUDE']:
                    if checkRange(rowdata['LATITUDE'], validRanges['LATITUDE']):
                        if checkRange(rowdata['LONGITUDE'], validRanges['LONGITUDE']):
                            #
                            # Wind direction (compass heading) to azimuth
                            # We have a choice of wind direction columns: choose WDIR1 if available;
                            # otherwise use WDIR2
                            #
                            try:
                                rowdata['WDIR'] = rowdata['WDIR1'] or rowdata['WDIR2']
                                rowdata.pop('WDIR1')
                                rowdata.pop('WDIR2')
                            except KeyError:
                                try:
                                    rowdata['WDIR'] = rowdata['WDIR1']
                                    rowdata.pop('WDIR1')
                                except KeyError:
                                    try:
                                        rowdata['WDIR'] = rowdata['WDIR2']
                                        rowdata.pop('WDIR2')
                                    except KeyError:
                                        pass
                            #
                            # Wind speed
                            # We have a choice of wind speed columns: choose WSPD1 if available;
                            # otherwise use WSPD2
                            #
                            try:
                                rowdata['WSPD'] = rowdata['WSPD1'] or rowdata['WSPD2']
                                rowdata.pop('WSPD1')
                                rowdata.pop('WSPD2')
                            except KeyError:
                                try:
                                    rowdata['WSPD'] = rowdata['WSPD1']
                                    rowdata.pop('WSPD1')
                                except KeyError:
                                    try:
                                        rowdata['WSPD'] = rowdata['WSPD2']
                                        rowdata.pop('WSPD2')
                                    except KeyError:
                                        pass
                            #
                            # Water depth  ASSUME units are FEET; convert to METERS
                            #
                            try:
                                rowdata['DEPTH'] = float(rowdata['DEPTH']) * F2M
                                qadata['DEPTH'] = QA_CHANGED
                                qacomments['DEPTH'] = 'Converted from feet to meters'
                            except (TypeError,ValueError):
                                if rowdata['DEPTH'].lower() == 'surface':
                                    rowdata['DEPTH'] = 0.0
                                    qadata['DEPTH'] = QA_CHANGED
                                    qacomments['DEPTH'] = 'Converted from "surface" to 0.0'
                                else:
                                    rowdata.pop('DEPTH')
                            #
                            # Determine ecosystem/ecoregion 
                            #
                            regions = findEcoRegionForCoords(rowdata['LONGITUDE'],rowdata['LATITUDE'])
                            try:
                                rowdata['ECOSYSTEM'] = regions[0][0].title()
                            except IndexError:
                                pass
                            try:
                                rowdata['ECOREGION'] = regions[1][0].title()
                            except IndexError:
                                pass
                            #
                            # QA the data
                            #
                            doQA(rowdata, qadata, qacomments)
                            #
                            # Remove items with empty string values or not in termMap.
                            #
                            for key in rowdata.keys():
                                if rowdata[key] == '':
                                    rowdata.pop(key)
    
                            for key in rowdata.keys():
                                if key not in termMap[state].values():
                                   rowdata.pop(key)
                            #
                            # Bin level (category)
                            #
                            rowdata['CATEGORIES'][organismName] =  getBinLevel(rowdata['CELLCOUNTS'][organismName], bin_levels[organismName])
    
                            yield rowdata, qadata, qacomments
                        else:
                            logging.warning('Longitude out of range: %f (valid range %f - %f) -- record discarded'%(rowdata['LONGITUDE'],
                                                                                                                    validRanges['LONGITUDE'][0],
                                                                                                                    validRanges['LONGITUDE'][1]))
                    else:
                        logging.warning('Latitude out of range: %f (valid range %f - %f) -- record discarded'%(rowdata['LATITUDE'],
                                                                                                               validRanges['LATITUDE'][0],
                                                                                                               validRanges['LATITUDE'][1]))
                else:
                    logging.warning('***Row %d:  Date and/or location missing -- ignored'%(ctr))
            else:
                logging.warning('*** Row %d: Header/value mismatch (expected %d, got %d) -- discarded'%(ctr, len(headers), len(vals)))
    except IOError, msg:
        logging.warning('IOError encountered.  %s'%str(msg))

#---------------------------------------------------------------------------------------------

def processTextMS(fileName, organisms, bin_levels, options, config, infoBroker):
    """
Description:    Process Mississippi HABS data from tab-delimited text files.
Input:          fileName:   <string>    Name of the text file.
                organisms:  <list>      List of ORGANISM records from the database
                bin_levels: <list>      List of BIN_LEVEL records from the database
                options:    <obj>       OptionParser (command-line options) object.
                config:     <obj>       ConfigParser application configuration data.
                infoBroker: <obj>       XMLRPC ServerProxy object.
Output:         None.
Return Value:   This method is a generator.  For each valid data line it returns a Tuple (rowdata, qadata, qacomments).
    """

    state = 'MS'
    #
    # Get the current time.
    #
    now = time.time()
    #
    # 
    #
    try:
        #
        # Open the file and Sniff the dialect
        #
        fp = open(fileName, 'r')
        dialect = csv.Sniffer().sniff(fp.readline())
        fp.seek(0)
        reader = csv.reader(fp, dialect=dialect)

        if options.startingRow < 1:
            options.startingRow = 1;
        #
        # Process the remaining rows in worksheet, beginning with 'startingRow'
        #
        logging.info('Getting headers...')
        #
        # Read column headers and map them to database terms
        #   ASSUME row 0 contains the column headers
        #
        headers = [x.strip() for x in reader.next()]
        for ndx in xrange(len(headers)):
           try:
               headers[ndx] = termMap[state][headers[ndx]]
           except KeyError:
               pass
        #
        # Advance the file pointer to 'startingRow'
        #
        for ctr in xrange(options.startingRow-1):
            reader.next()

        ctr = options.startingRow - 1
        for line in reader:
            ctr += 1
            logging.info('Row %d'%(ctr))
            #
            # Accumulate data values in a list, and build a dictionary using the headers & vals
            #
            vals = [x.strip() for x in line]
            if len(vals) == len(headers):
                #
                # Convert Unicode to ASCII; ignore any 'special' chars (I hope)
                #
                for ndx in xrange(len(vals)):
                   if type(vals[ndx]) == types.UnicodeType:
                       vals[ndx] = codecs.decode(vals[ndx].strip(), 'ascii', 'replace')
                rowdata = (dict(zip(headers, vals)))
                #
                # HACK
                #
                rowdata['Collection Type'] = 'surface grab'
                if rowdata['Collection Type'].lower() == 'surface grab':
                    rowdata.update({'STATE':state, 'CELLCOUNTS':{}, 'ORGANISM_IDS': {}, 'CATEGORIES':{}})
                    #
                    # So far these files report only K. brevis counts, but we will need to support multiple
                    # species in future.  We behave as if multiple species are possible in these data.
                    # Also K. brevis is not explicitly indicated in the data; we ASSUME it here.
                    #
                    genus = 'Karenia'
                    species = 'brevis'
                    organismName = '%s %s'%(genus, species)
                    #
                    # Get the database ID for this organism
                    #
                    for organism in organisms:
                        if organism['ORGANISM_NAME'] == organismName:
                            rowdata['ORGANISM_IDS'][organismName] = organism['OBJECTID']
                            break
                    logging.debug('Raw data: %s'%pprint.pformat(rowdata))
                    #
                    # Initialize all QA variables to QA_MISSING and all comments to empty strings
                    #
                    qadata, qacomments = initQAVariables([organismName])
    
                    dateTime = ('%s %s'%(rowdata['DATE'], rowdata['TIME'])).strip()
                    logging.debug('Incoming datetime %s'%`dateTime`)
                    rowdata['DATE'] = None
                    for fmt in dateFormats:
                        try:
                            rowdata['DATE'] = time.mktime(time.strptime(dateTime, fmt))
                            logging.debug('Successfully parsed datetime')
                            #
                            # Is the sample time in the future?  This can happen if the column or cell is misformatted; e.g., the observer
                            # entered '0900' vice '9:00' and/or the column wasn't formatted for time values. 
                            #
                            if rowdata['DATE'] > now:
                                logging.warning('***Sample time is in the future!  Incoming time value was %s. -- ignored'%`rowdata['TIME']`)
                                continue
                            rowdata.pop('TIME')
                            #
                            # Hacktastic:  time of day in the input is local, without a timezone hint.  We want to store
                            # time as UTC.
                            #
                            # First run the timestamp through time.localtime() to see if DST applies.
                            #
                            dst = time.localtime(rowdata['DATE'])[8]
                            #
                            # Mississippi is entirely in the Central timezone (-6 hours from UTC).
                            #
                            tzoffset = -6*3600 + dst*3600
                            rowdata['DATE'] -= tzoffset
                            break
                        except ValueError:
                            pass 
                    #
                    # Latitude
                    #
                    rowdata['LATITUDE'] = DMS2DD(rowdata['LATITUDE'])
                    #
                    # Longitude
                    #
                    rowdata['LONGITUDE'] = DMS2DD(rowdata['LONGITUDE'])
                    #
                    # Longitudes are all West longitudes (so far) but are not signed in some cases.  Flip the sign
                    # if necessary...
                    #
                    if rowdata['LONGITUDE'] > 0.0:
                        rowdata['LONGITUDE'] = -rowdata['LONGITUDE']
                    #
                    # Cellcount, by organism.
                    #    Sometimes contains text like 'inconclusive' or 'TNTC' (Too Numerous To Count)
                    #
                    try:
                        rowdata['CELLCOUNTS'][organismName] = float(rowdata['CELLCOUNT'].replace(',',''))
                    except ValueError, msg:
                        logging.warning('ValueError: CELLCOUNTS %s'%str(msg))
                        rowdata['CELLCOUNTS'][organismName] = 0.0
                    #
                    # Must have a date and a position to continue
                    #
                    logging.debug('Parsed data: %s'%pprint.pformat(rowdata))
                    if rowdata['DATE'] and rowdata['LATITUDE'] and rowdata['LONGITUDE']:
                        if checkRange(rowdata['LATITUDE'], validRanges['LATITUDE']):
                            if checkRange(rowdata['LONGITUDE'], validRanges['LONGITUDE']):
                                #
                                # Wind direction (compass heading) to azimuth
                                # We have a choice of wind direction columns: choose WDIR1 if available;
                                # otherwise use WDIR2
                                #
                                try:
                                    rowdata['WDIR'] = rowdata['WDIR1'] or rowdata['WDIR2']
                                    rowdata.pop('WDIR1')
                                    rowdata.pop('WDIR2')
                                except KeyError:
                                    try:
                                        rowdata['WDIR'] = rowdata['WDIR1']
                                        rowdata.pop('WDIR1')
                                    except KeyError:
                                        try:
                                            rowdata['WDIR'] = rowdata['WDIR2']
                                            rowdata.pop('WDIR2')
                                        except KeyError:
                                            pass
                                #
                                # Wind speed
                                # We have a choice of wind speed columns: choose WSPD1 if available;
                                # otherwise use WSPD2
                                #
                                try:
                                    rowdata['WSPD'] = rowdata['WSPD1'] or rowdata['WSPD2']
                                    rowdata.pop('WSPD1')
                                    rowdata.pop('WSPD2')
                                except KeyError:
                                    try:
                                        rowdata['WSPD'] = rowdata.pop('WSPD1')
                                    except KeyError:
                                        try:
                                            rowdata['WSPD'] = rowdata.pop('WSPD2')
                                        except KeyError:
                                            pass
                                #try:
                                #    float(rowdata['WSPD'])
                                #except ValueError:
                                #    rowdata.pop('WSPD')
                                #except KeyError:
                                #    pass
                                #
                                # Water depth
                                #
                                try:
                                    rowdata['DEPTH'] = float(rowdata['DEPTH']) * F2M
                                    qadata['DEPTH'] = QA_CHANGED
                                    qacomments['DEPTH'] = 'Converted from feet to meters'
                                except (TypeError,ValueError):
                                    if rowdata['DEPTH'].lower() == 'surface':
                                        rowdata['DEPTH'] = 0.0
                                        qadata['DEPTH'] = QA_CHANGED
                                        qacomments['DEPTH'] = 'Converted from "surface" to 0.0'
                                    else:
                                        rowdata.pop('DEPTH')
                                except KeyError:
                                    pass
                                #
                                # Determine ecosystem/ecoregion 
                                #
                                regions = findEcoRegionForCoords(rowdata['LONGITUDE'],rowdata['LATITUDE'])
                                try:
                                    rowdata['ECOSYSTEM'] = regions[0][0].title()
                                except IndexError:
                                    pass
                                try:
                                    rowdata['ECOREGION'] = regions[1][0].title()
                                except IndexError:
                                    pass
                                #
                                # QA the data
                                #
                                doQA(rowdata, qadata, qacomments)
                                #
                                # Remove items with empty string values or not in termMap.
                                #
                                for key in rowdata.keys():
                                    if rowdata[key] == '':
                                        rowdata.pop(key)
    
                                for key in rowdata.keys():
                                    if key not in termMap[state].values():
                                       rowdata.pop(key)
                                #
                                # Bin level (category)
                                #
                                rowdata['CATEGORIES'][organismName] =  getBinLevel(rowdata['CELLCOUNTS'][organismName], bin_levels[organismName])
    
                                yield rowdata, qadata, qacomments
                            else:
                                logging.warning('Longitude out of range: %f (valid range %f - %f) -- record discarded'%(rowdata['LONGITUDE'],
                                                                                                                        validRanges['LONGITUDE'][0],
                                                                                                                        validRanges['LONGITUDE'][1]))
                        else:
                            logging.warning('Latitude out of range: %f (valid range %f - %f) -- record discarded'%(rowdata['LATITUDE'],
                                                                                                                   validRanges['LATITUDE'][0],
                                                                                                                   validRanges['LATITUDE'][1]))
                    else:
                        logging.warning('***Row %d:  Date and/or location missing -- ignored'%(ctr))
                else:
                    logging.info('Collection type not "surface grab" -- ignoring')
            else:
                logging.warning('*** Row %d: Header/value mismatch (expected %d, got %d) -- discarded'%(ctr, len(headers), len(vals)))
    except IOError, msg:
        logging.warning('IOError encountered.  %s'%str(msg))
