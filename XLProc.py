#-------------------------------------------------------------------------------------
#
# Methods for processing spreadsheets from data providers (by state)
#   processSpreadsheetTX:   spreadsheets from Texas HABS monitoring entities (various)
#   processSpreadsheetMS:   spreadsheets from Mississippi DMR
#   processSpreadsheetFL:   spreadsheets from Florida
#
#-------------------------------------------------------------------------------------
import logging, codecs, types, time, sys, pprint
try:
    import xlrd
except ImportError:
    pass
from regexes import *
from constants import *
from utilities import *
from QA import *
from ranges import validRanges
from validation import checkRange
from EcoRegionChecker import findEcoRegionForCoords

#-------------------------------------------------------------------------------------

def getFLCountDataFromWorkSheet(workSheet, organisms):
    """
    Description:    Return cellcount data (by sample id) for organisms of interest.
    Input:          worksheet:  <obj>   xlrd Worksheet object containing the cellcount data.
                    organisms:  <list>  List of ORGANISM records from the database.
    Output:         None.
    Return Valuie:  Dictionary whose keys are sample ids and whose values are Dictionaries of
                    cellcount data.
    Discussion:     Florida reports cellcounts for multiple species, which are stored in a separate
                    worksheet from the temporal, spatial and environmental parameters.  Cellcount
                    observations are related to the geospatial records by a unique sample id.
                    Using the list of ORGANISM records, this method reads the worksheet and collects
                    all cellcount data identified by a given sample id into a data structure for use
                    by processSpreadsheetFL().
                    This looks like
                    {'8193324': {'Karenia brevis':1000,
                                 'Gyrodinium spiralis':100,
                                 'Gyrodinium spp.':0},
                     '8293325': {'Karenia brevis':1000}...
                    }
    """

    retval = {}
    #
    # Build list of organism names (ex. "Karenia brevis") from the ORGANISM records
    #
    orgNames = ['%s %s'%(x['GENUS'],x['SPECIES']) for x in organisms]
    #
    # Assume column headings are in row 0
    #
    headers = [cell.value.upper() for cell in workSheet.row(0)]
    for row in xrange(1, workSheet.nrows):
        #
        # For each data row, zip the headers and vals into a Dictionary, then
        # see if the species name is one we're interested in
        #
        vals = [cell.value for cell in workSheet.row(row)]
	rowDict = dict(zip(headers, vals))
        if rowDict['NAME'] in orgNames:
            try:
                #
                # Accumulate the cellcount data into the return value
                #
                try:
                    retval[rowDict['SAMPLE_ID']].update({rowDict['NAME']:int(rowDict['COUNT'])})
                except KeyError:
                    retval[rowDict['SAMPLE_ID']] = {rowDict['NAME']:int(rowDict['COUNT'])}
            except ValueError:
                pass

    return retval

#-------------------------------------------------------------------------------------

def processSpreadsheetFL(fileName, organisms, bin_levels, options, config):
    """
    Description:    Process spreadsheets provided by Florida.
    Inptut:         fileName:   <string> Name of the spreadsheet file.
                    organisms:  <list>   List of ORGANISM records from the database.
                    bin_levels: <list>   List of BIN_LEVEL records from the database.
                    options:    <obj>    OptionParser command-line options.
                    config:     <obj>    ConfigParser application configuration data.
    Output:         Logging messages.
    Return Value:   This method is a generator.  For each line processed from the data file,
                    this method yields a 3-Tuple of Dictionaries containing observed parameters,
                    quality-assurance values, and quality-assurance comments.
    """

    state = 'FL'
    now = time.time()
    try:
        workBook = xlrd.open_workbook(fileName)
        sampleSheet = workBook.sheet_by_name('sample')
        countData = getFLCountDataFromWorkSheet(workBook.sheet_by_name('count'), organisms)
        #
        # Validate/correct user-defined starting row
        #    ASSUME row 0 contains column headers, data begin at row 1 (row 2 in spreadsheet view)
        #
        if options.startingRow > sampleSheet.nrows:
            logging.critical('Starting row (%d) greater than number of rows in spreadsheet (%d).'%(options.startingRow, sampleSheet.nrows))
            sys.exit(-1)
        elif options.startingRow < 1:
            options.startingRow = 1;
        #
        # Process the remaining rows in worksheet, beginning with 'startingRow'
        #
        logging.info('Getting headers...')
        #
        # Accumulate column headers in a list while mapping them to database terms
        #     ASSUME row 0 contains the column headers
        #
        headers = []
        for cell in sampleSheet.row(0):
           try:
               headers.append(termMap[state][cell.value])
           except KeyError:
               headers.append(cell.value)

        logging.info('Processing rows %d-%d.'%(options.startingRow, sampleSheet.nrows))
        for row in xrange(options.startingRow-1, sampleSheet.nrows):
           logging.info('Row %d'%(row+1))
           #
           # Initialize
           #
           rowdata = {'STATE':state, 'AGENCY':'FWC', 'CELLCOUNTS':{}, 'ORGANISM_IDS': {}, 'CATEGORIES': {}}
           vals = [cell.value for cell in sampleSheet.row(row)]
           for ndx in xrange(len(vals)):
              if type(vals[ndx]) in [types.UnicodeType, types.StringType]:
                  vals[ndx] = codecs.encode(vals[ndx].strip(), 'ascii', 'replace')
           rowdata.update(dict(zip(headers, vals)))
           #
           # Term collison here:  FL spreadsheet has a 'DESCRIPTION' column, but we
           # use 'DESCRIPTION' in the database to hold the sampling site name.
           # So we do an explicit swap here.
           #
           rowdata['DESCRIPTION'] = rowdata.pop('LOCATION')
           #
           # Excel date to Unix epoch
           #
           rowdata['DATE'] = excel2epoch(rowdata['DATE'], workBook.datemode)
           try:
               rowdata['DATE'] += float(rowdata['TIME']) * SECS_IN_DAY
           except (ValueError, TypeError):
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
           if rowdata['PROOF_DATE']:
               try:
                   #
                   # Store cellcounts and organism ids
                   #
                   rowdata['CELLCOUNTS'].update(countData[rowdata['HABSOS_ID']])
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

                   yield rowdata, qadata, qacomments
               except KeyError, msg:
                   logging.warning('KeyError: %s'%str(msg))
           else:
               logging.info('%s:  No proof date.'%rowdata['HABSOS_ID'])
               logging.info('----------------------------------------')
    except xlrd.XLRDError, msg:
        logging.warning('XLRDError encountered.  %s'%str(msg))

#-------------------------------------------------------------------------------------

def processSpreadsheetTX(fileName, organisms, bin_levels, options, config):
    """
    Description:    Process spreadsheets provided by Texas.
    Inptut:         fileName:   <string> Name of the spreadsheet file.
                    organisms:  <list>   List of ORGANISM records from the database.
                    bin_levels: <list>   List of BIN_LEVEL records from the database.
                    options:    <obj>    OptionParser command-line options.
                    config:     <obj>    ConfigParser application configuration data.
    Output:         Logging messages.
    Return Value:   This method is a generator.  For each line processed from the data file,
                    this method yields a 3-Tuple of Dictionaries containing observed parameters,
                    quality-assurance values, and quality-assurance comments.
    """

    state = 'TX'
    #
    # Get the current time.
    #
    now = time.time()
    #
    # ASSUME the first worksheet in the workbook contains cellcount info.
    #
    try:
        workBook = xlrd.open_workbook(fileName)
        workSheet = workBook.sheet_by_index(0)
        #
        # Validate/correct user-defined starting row
        #    ASSUME row 0 contains column headers, data begin at row 1 (row 2 in spreadsheet view)
        #
        if options.startingRow > workSheet.nrows:
            logging.critical('Starting row (%d) greater than number of rows in spreadsheet (%d).'%(options.startingRow, workSheet.nrows))
            sys.exit(-1)
        elif options.startingRow < 1:
            options.startingRow = 1;
        #
        # Process the remaining rows in worksheet, beginning with 'startingRow'
        #
        logging.info('Getting headers...')
        #headers = [cell.value for cell in workSheet.row(0)]
        #
        # Accumulate column headers in a list while mapping them to database terms
        #     ASSUME row 0 contains the column headers
        #
        headers = []
        for cell in workSheet.row(0):
           try:
               headers.append(termMap[state][cell.value])
           except KeyError:
               headers.append(cell.value)

        logging.info('Processing rows %d-%d.'%(options.startingRow, workSheet.nrows))

        for ctr in xrange(options.startingRow-1, workSheet.nrows):
            logging.info('Row %d'%(ctr+1))
            #
            # Accumulate cell values in a list, and build a dictionary using the headers & vals
            #
            cells = workSheet.row(ctr)
            vals = [cell.value for cell in cells]
            for ndx in xrange(len(vals)):
               if type(vals[ndx]) in [types.UnicodeType, types.StringType]:
                   vals[ndx] = codecs.encode(vals[ndx].strip(), 'ascii', 'replace')

            rowData = dict(zip(headers, vals))
            cellData = dict(zip(headers, cells))
            rowData.update({'STATE':state, 'CELLCOUNTS':{}, 'ORGANISM_IDS': {}, 'CATEGORIES':{}})
            #
            # So far these spreadsheets report only K. brevis counts, but we will need to support multiple
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
                    rowData['ORGANISM_IDS'][organismName] = organism['OBJECTID']
                    break
            #
            # Initialize all QA variables to QA_MISSING and all comments to empty strings
            #
            qadata, qacomments = initQAVariables([organismName])
            goAhead = True
            #
            # Already in HABSOS?  Then we don't need to process this one.
            #
            try:
                goAhead = not rowData['Entered in HABSOS'].lower() == 'y'
                if not goAhead:
                    logging.warning('***Row %d already entered in HABSOS -- ignored'%(ctr+1))
            except KeyError:
                pass

            if goAhead:
                #
                # Convert date/time to Unix epoch
                #
                try:
                    if cellData['DATE'].ctype == xlrd.XL_CELL_DATE:
                        rowData['DATE'] = excel2epoch(rowData['DATE'], workBook.datemode)
                    elif cellData['DATE'].ctype == xlrd.XL_CELL_TEXT:
                        rowData['DATE'] = date2epoch(rowData['DATE'])
                    else:
                        rowData['DATE'] = date2epoch(str(rowData['DATE']))
                except ValueError:
                    pass
                #
                # If we have a non-empty time of day field, convert it to seconds and add it to the
                # date field so the date field will account for time of day as well.
                #
                try:
                   if cellData['TIME'].ctype == xlrd.XL_CELL_DATE:
                       #
                       # In Excel, time of day is represented as a fractional day; e.g., 8:00 AM is represented
                       # as 0.3333333, noon is 0.5, etc.
                       #
                       rowData['DATE'] += float(rowData['TIME']) * SECS_IN_DAY
                   #
                   # If the cell's data type is text or number, try to convert to epoch seconds using
                   # the time module
                   #
                   elif cellData['TIME'].ctype == xlrd.XL_CELL_TEXT:
                       rowData['DATE'] += time2epoch(rowData['TIME'])
                   else:
                       rowData['DATE'] += time2epoch(str(rowData['TIME']))
                except (ValueError, TypeError):
                   #
                   # The value may be missing, or it may be one of 'morning','afternoon','evening','mid-pm'
                   # Arbitrarily define 'morning' as 0900;
                   #                    'afternoon' and 'mid-pm' as 1500;
                   #                    'evening' as 1800. 
                   #
                   if rowData['TIME'] in ['morning','am']:
                      rowData['TIME'] = 0.375 # 0900
                   elif rowData['TIME'] in ['afternoon', 'mid-pm']:
                      rowData['TIME'] = 0.625 # 1500
                   elif rowData['TIME'] == 'evening':
                      rowData['TIME'] = 0.75  # 1800
                   #
                   # Try again and bail if exception thrown
                   #
                   try:
                      rowData['DATE'] += rowData['TIME'] * SECS_IN_DAY
                   except (ValueError, TypeError):
                      pass
                #
                # Is the sample time in the future?  This can happen if the column or cell is misformatted; e.g., the observer
                # entered '0900' vice '9:00' and/or the column wasn't formatted for time values. 
                #
                if rowData['DATE'] > now:
                    logging.warning('***Sample time is in the future!  Incoming time value was %s. -- ignored'%`rowData['TIME']`)
                    continue
                rowData.pop('TIME')
                #
                # Hacktastic:  time of day in the input is local, without a timezone hint.  We want to store
                # time as UTC.
                #
                # First run the timestamp through time.localtime() to see if DST applies.
                #
                dst = time.localtime(rowData['DATE'])[8]
                #
                # Texas is entirely in the Central timezone (-6 hours from UTC).
                #
                tzoffset = -6*3600 + dst*3600
                rowData['DATE'] -= tzoffset
                #
                # Latitude
                #
                rowData['LATITUDE'] = DMS2DD(rowData['LATITUDE'])
                #
                # Longitude
                #
                rowData['LONGITUDE'] = DMS2DD(rowData['LONGITUDE'])
                #
                # Longitudes are all West longitudes (so far) but are not signed in most cases.  Flip the sign
                # if necessary...
                #
                if rowData['LONGITUDE'] > 0.0:
                    rowData['LONGITUDE'] = -rowData['LONGITUDE']
                #
                # Cellcount, by organism.
                #    Sometimes contains text like 'inconclusive' or 'TNTC' (Too Numerous To Count)
                #
                logging.debug('cellcount %s'%rowData['CELLCOUNT'])
                try:
                    rowData['CELLCOUNTS'][organismName] = float(rowData['CELLCOUNT'])
                    #
                    # Must have a date and a position to continue
                    #
                    if rowData['DATE'] and rowData['LATITUDE'] and rowData['LONGITUDE']:
                        if checkRange(rowData['LATITUDE'], validRanges['LATITUDE']):
                            if checkRange(rowData['LONGITUDE'], validRanges['LONGITUDE']):
                                #
                                # Wind direction (compass heading) to azimuth
                                # We have a choice of wind direction columns: choose WDIR1 if available;
                                # otherwise use WDIR2
                                #
                                try:
                                    rowData['WDIR'] = rowData['WDIR1'] or rowData['WDIR2']
                                    rowData.pop('WDIR1')
                                    rowData.pop('WDIR2')
                                except KeyError:
                                    try:
                                        rowData['WDIR'] = rowData['WDIR1']
                                        rowData.pop('WDIR1')
                                    except KeyError:
                                        try:
                                            rowData['WDIR'] = rowData['WDIR2']
                                            rowData.pop('WDIR2')
                                        except KeyError:
                                            pass
                                #
                                # Wind speed
                                # We have a choice of wind speed columns: choose WSPD1 if available;
                                # otherwise use WSPD2
                                #
                                try:
                                    rowData['WSPD'] = rowData['WSPD1'] or rowData['WSPD2']
                                    rowData.pop('WSPD1')
                                    rowData.pop('WSPD2')
                                except KeyError:
                                    try:
                                        rowData['WSPD'] = rowData['WSPD1']
                                        rowData.pop('WSPD1')
                                    except KeyError:
                                        try:
                                            rowData['WSPD'] = rowData['WSPD2']
                                            rowData.pop('WSPD2')
                                        except KeyError:
                                            pass
                                #
                                # Water depth
                                #
                                try:
                                    rowData['DEPTH'] = float(rowData['DEPTH']) * F2M
                                    qadata['DEPTH'] = QA_CHANGED
                                    qacomments['DEPTH'] = 'Converted from feet to meters'
                                except (TypeError,ValueError):
                                    if rowData['DEPTH'].lower() == 'surface':
                                        rowData['DEPTH'] = 0.0
                                        qadata['DEPTH'] = QA_CHANGED
                                        qacomments['DEPTH'] = 'Converted from "surface" to 0.0'
                                    else:
                                        rowData.pop('DEPTH')
                                #
                                # Determine ecosystem/ecoregion 
                                #
                                regions = findEcoRegionForCoords(rowData['LONGITUDE'],rowData['LATITUDE'])
                                try:
                                    rowData['ECOSYSTEM'] = regions[0][0].title()
                                except IndexError:
                                    pass
                                try:
                                    rowData['ECOREGION'] = regions[1][0].title()
                                except IndexError:
                                    pass
                                #
                                # QA the data
                                #
                                doQA(rowData, qadata, qacomments)
                                #
                                # Remove items with empty string values or not in termMap.
                                #
                                for key in rowData.keys():
                                    if rowData[key] == '':
                                        rowData.pop(key)

                                for key in rowData.keys():
                                    if key not in termMap[state].values():
                                       rowData.pop(key)
                                #
                                # Bin level (category)
                                #
                                # 20140806  David E. Sallis
                                #   Texas reports cellcounts in cells/mL; database units are cells/L.  So multiply cellcounts by 1000.0.
                                #
                                rowData['CELLCOUNTS'][organismName] *= 1000.0
                                rowData['CATEGORIES'][organismName] =  getBinLevel(rowData['CELLCOUNTS'][organismName], bin_levels[organismName])
                                qadata['CELLCOUNTS'][organismName] = QA_CHANGED
                                qacomments['CELLCOUNTS'][organismName] = "Extrapolated from original cells/mL value"

                                yield rowData, qadata, qacomments
                            else:
                                logging.warning('Longitude out of range: %f (valid range %f - %f) -- record discarded'%(rowData['LONGITUDE'],
                                                                                                                        validRanges['LONGITUDE'][0],
                                                                                                                        validRanges['LONGITUDE'][1]))
                        else:
                            logging.warning('Latitude out of range: %f (valid range %f - %f) -- record discarded'%(rowData['LATITUDE'],
                                                                                                                   validRanges['LATITUDE'][0],
                                                                                                                   validRanges['LATITUDE'][1]))

                    else:
                        logging.warning('***Row %d:  Date and/or location missing -- ignored'%(ctr+1))
                except ValueError, msg:
                    logging.warning('ValueError %s--RECORD NOT PROCESSED'%str(msg))
    except xlrd.XLRDError, msg:
        logging.warning('XLRDError encountered.  %s'%str(msg))

#-------------------------------------------------------------------------------------

def processSpreadsheetMS(fileName, organisms, bin_levels, options, config):
    """
    Description:    Process spreadsheets provided by Florida.
    Inptut:         fileName:   <string> Name of the spreadsheet file.
                    organisms:  <list>   List of ORGANISM records from the database.
                    bin_levels: <list>   List of BIN_LEVEL records from the database.
                    options:    <obj>    OptionParser command-line options.
                    config:     <obj>    ConfigParser application configuration data.
    Output:         Logging messages.
    Return Value:   This method is a generator.  For each line processed from the data file,
                    this method yields a 3-Tuple of Dictionaries containing observed parameters,
                    quality-assurance values, and quality-assurance comments.
    """

    state = 'MS'
    stationPositions = {'2-14':{'latitude':30.28495,'longitude':-89.23712},
                        '2-15B':{'latitude':30.19643,'longitude':-89.22527}}
    #
    # Get the current time.
    #
    now = time.time()
    try:
        workBook = xlrd.open_workbook(fileName)
        for sheet in workBook.sheets():
            envHeaders  = []
            if sheet.name.endswith('Sample Count'):
                logging.info('Processing worksheet %s...'%sheet.name)
                #
                # Sample Count worksheets will be accompanied by a related Environmental Data worksheet
                # Worksheet names are prefixed with station ID
                # After getting the station ID from the sample count worksheet name, read the
                # Environmental Data worksheet to get parameter names and row indices
                #
                stationId = token_regex.findall(sheet.name)[0].strip()
                coords = stationPositions[stationId]
                envDataSheetName = '%s Environmental Data'%stationId
                logging.info('  Looking for environmenta data worksheet %s...'%envDataSheetName)
                envDataSheet = workBook.sheet_by_name(envDataSheetName)
                logging.info('  OK')
                #
                # ASSUME row 'headers' (parameter names and units) are in column 0
                #
                envHeaderCol = envDataSheet.col(0)
                #
                # Read each row (skipping row 0), storing only non-empty values
                #
                logging.info('  Building environmental data headers...')
                for envRowIndex in xrange(1, envDataSheet.nrows):
                   if envHeaderCol[envRowIndex].ctype != xlrd.XL_CELL_EMPTY:
                       envHeaderVal = envHeaderCol[envRowIndex].value
                       #
                       # Remove any parentheticals; typically these are units
                       #
                       for x in parenthetical_regex.findall(envHeaderVal):
                          envHeaderVal = envHeaderVal.replace(x, '')
                       #
                       # Store both the parameter name and the row index
                       # from which it came
                       #
                       try:
                           envHeaders.append((termMap[state][envHeaderVal.strip()], envRowIndex))
                       except KeyError:
                          envHeaders.append((envHeaderVal, envRowIndex))
                #
                # Back to the Sample Count sheet: get the first column--contains the organism names
                #
                logging.info('  Building species list...')
                cells = [cell for cell in sheet.col(0)]
                knownSpecies = []
                commentRowIndex = None
                collectorRowIndex = None
                for cell in cells:
                    #
                    # Some of the organism names are prefixed with asterisks for some damn reason
                    #
                    cell.value = cell.value.replace('*','').strip()
                    #
                    # Check the organism name against the keys in bin_levels.
                    # These are our 'known species'.
                    # If no match, discard.
                    #
                    if cell.value in bin_levels:
                        knownSpecies.append((cell.value, cells.index(cell)))
                    elif cell.value.lower() == 'comments':
                        commentRowIndex = cells.index(cell)
                    elif cell.value.lower() == 'sample counted by':
                        collectorRowIndex = cells.index(cell)
                logging.info('  Known species:')
                for x in knownSpecies:
                    logging.info('    %s'%x[0])
                #
                # Process each succeeding column in Sample Count
                #
                for cellcountColIndex in xrange(1, sheet.ncols):
                    logging.info('  Column %d'%cellcountColIndex)
                    cellcountCol = sheet.col(cellcountColIndex)
                    #
                    # If the type of the first cell in the column is CELL_DATE, this is
                    # the flag that tells us here be data
                    #
                    if cellcountCol[0].ctype == xlrd.XL_CELL_DATE:
                       rowData = {'CELLCOUNTS':{},
                                  'ORGANISM_IDS': {},
                                  'CATEGORIES': {},
                                  'AGENCY':'MSDMR',
                                  'STATE':state,
                                  'DESCRIPTION':stationId,
                                  'LATITUDE':coords['latitude'],
                                  'LONGITUDE':coords['longitude']}
                       #
                       # Initialize QA variables
                       #
                       qadata, qacomments = initQAVariables([k[0] for k in knownSpecies])
                       #
                       # Convert the Excel date into a Unix time tuple, extend it to 9 elements, and convert to
                       # Unix epoch timestamp
                       #
                       try:
                           rowData['DATE'] = excel2epoch(cellcountCol[0].value, workBook.datemode)
                       except ValueError:
                           pass
                       #
                       # Store any cellcount data using our 'known species' list
                       #
                       for organismName, rowIndex in knownSpecies:
                           rowData['CELLCOUNTS'][organismName] = cellcountCol[rowIndex].value
                           if rowData['CELLCOUNTS'][organismName] == '':
                              rowData['CELLCOUNTS'][organismName] = 0.0
                           #
                           # 20140806  David E. Sallis
                           #   MS reports cellcounts in cells/mL; database units are cells/L.  So multiply MS cellcounts by 1000.0.
                           #
                           rowData['CELLCOUNTS'][organismName] *= 1000.0
                           #
                           # Also organism IDs and categorization
                           #
                           rowData['ORGANISM_IDS'][organismName] = [o['OBJECTID'] for o in organisms if o['ORGANISM_NAME'] == organismName][0]
                           rowData['CATEGORIES'][organismName] = getBinLevel(rowData['CELLCOUNTS'][organismName], bin_levels[organismName])
                       #
                       # Using the date from the cellcount worksheet, find the corresponding 
                       # environmental data in the environmental data workseet 
                       # Sequential search thru the columns...
                       #
                       for envColIndex in xrange(envDataSheet.ncols):
                           envDataCol = envDataSheet.col(envColIndex)
                           if envDataCol[0].ctype == xlrd.XL_CELL_DATE and envDataCol[0].value == cellcountCol[0].value:
                              #
                              # Found the corresponding environmental data record.  Store the
                              # parameter names and their values in 'rowData'
                              #
                              for envHeader in envHeaders:
                                 rowData[envHeader[0]] = envDataCol[envHeader[1]].value
                              #
                              # Convert the time o' day (not formatted as a 'Time' value Excel-wise, it's actually a
                              # float internally, but represented as an integer) into seconds and add it to the DATE
                              # parameter, thus giving us the date and time expressed as a Unix epoch timestamp
                              # 
                              try:
                                 timeVal = int(rowData.pop('TIME'))
                              except (KeyError, ValueError):
                                 timeVal = 0
                              rowData['DATE'] += ((timeVal / 100) * 3600) + ((timeVal % 100) * 60)
                              #
                              # Hacktastic:  time of day in the input is local, without a timezone hint.  We want to store
                              # time as UTC.
                              #
                              # First run the timestamp through time.localtime() to see if DST applies.
                              #
                              dst = time.localtime(rowData['DATE'])[8]
                              #
                              # Mississippi is entirely in the Central timezone (-6 hours from UTC).
                              #
                              tzoffset = -6*3600 + dst*3600
                              rowData['DATE'] -= tzoffset
                              #
                              # store the comment at this point.
                              #
                              if commentRowIndex is not None:
                                  rowData['COMMENTS'] = cellcountCol[commentRowIndex].value
                              #
                              # ID the data collector.
                              #
                              if collectorRowIndex is not None:
                                  try:
                                     rowData['COLLECTOR'] = MSDMRCollectors[cellcountCol[collectorRowIndex].value.split('/')[0]]
                                  except KeyError:
                                     logging.warning('    No data collector identified for this sample.')
                              #
                              # Determine ecosystem/ecoregion
                              #
                              regions = findEcoRegionForCoords(rowData['LONGITUDE'],rowData['LATITUDE'])
                              try:
                                  rowData['ECOSYSTEM'] = regions[0][0].title()
                              except IndexError:
                                  pass
                              try:
                                  rowData['ECOREGION'] = regions[1][0].title()
                              except IndexError:
                                  pass
                              #
                              # QA the data
                              #
                              doQA(rowData, qadata, qacomments)
                              #
		              # Remove items with empty string values or not in termMap.
		              #
                              for key in rowData.keys():
		                  if rowData[key] == '':
			              rowData.pop(key)

                              for key in rowData.keys():
                                  if key not in termMap[state].values():
                                     rowData.pop(key)
                              #
                              # Yield to caller
                              #
                              yield rowData, qadata, qacomments
    except xlrd.XLRDError, msg:
        logging.warning('  XLRDError encountered. %s'%str(msg))
