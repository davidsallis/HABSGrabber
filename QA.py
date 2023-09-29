#-------------------------------------------------------------------------------------
#
# QA.py
#
# Quality-assurance related functions
#
#-------------------------------------------------------------------------------------

import types, logging
from constants import *
from regexes import unsigned_number_regex
from ranges import validRanges
from validation import checkRange

#-------------------------------------------------------------------------------------

def initQAVariables(organismNames=[]):
    """
    Description:    Initialize a data structure containing QA parameters.
    Input:          organismNames:  <list>  Optional list of HABS organism names.
    Output:         None.
    Return Value:   Initialized data structures in a 2-tuple.
    """

    qadata = {'CELLCOUNTS':{}}
    qacomments = {'CELLCOUNTS':{}}
    qadata.update({'DEPTH':QA_MISSING,
                   'SALINITY':QA_MISSING,
                   'WDIR':QA_MISSING,
                   'WSPD':QA_MISSING,
                   'WTEMP':QA_MISSING,
                  })
    qacomments.update({'DEPTH':'',
                       'SALINITY':'',
                       'WDIR':'',
                       'WSPD':'',
                       'WTEMP':'',
                      })

    for name in organismNames:
        qadata['CELLCOUNTS'].update({name:QA_MISSING})
        qacomments['CELLCOUNTS'].update({name:''})

    return qadata, qacomments

#-------------------------------------------------------------------------------------

def doQA(rowdata, qadata, qacomments):
    """
    Description:    Perform QA checks on incoming HABS data.
    Input:          rowdata:     <dict>  Dictionary containing HABS parameters.
                    qadata:      <dict>  Dictionary containing numerical QA parameters corresponding to the HABS parameters.
                    qacomments:  <dict>  Dictionary containing any comments about the QA data.
    Output:         None.
    Return Value:   None.  The input dictionaries are modified in-place.
    """
    logging.info('    Performing QA checks...')
    #
    # Cell counts
    #
    for organismName in rowdata['CELLCOUNTS']:
        try:
            rowdata['CELLCOUNTS'][organismName] = float(rowdata['CELLCOUNTS'][organismName])
            qadata['CELLCOUNTS'][organismName] = QA_CORRECT
        except (TypeError,ValueError):
            try:
                rowdata['CELLCOUNTS'][organismName] = float(rowdata['CELLCOUNTS'][organismName].replace(',','').replace('+',''))
                qadata['CELLCOUNTS'][organismName] = QA_CHANGED
                qacomments['CELLCOUNTS'][organismName] = 'Cellcount converted from original formatted value'
            except (TypeError,ValueError):
                try:
                    rowdata['CELLCOUNTS'][organismName] = float(unsigned_number_regex.findall(rowdata['CELLCOUNTS'][organismName])[0])
                    qadata['CELLCOUNTS'][organismName] = QA_CHANGED
                    qacomments['CELLCOUNTS'][organismName] = 'Cellcount converted from original formatted value'
                except IndexError:
                    rowdata['CELLCOUNTS'][organismName] = 0.0
                    qadata['CELLCOUNTS'][organismName] = QA_MISSING
                    qacomments['CELLCOUNTS'][organismName] = 'Cellcount unexpected data in input'
    #
    # Water depth
    #
    try:
        rowdata['DEPTH'] = float(rowdata['DEPTH'])
        qadata['DEPTH'] = QA_CORRECT
    except ValueError:
        if rowdata['DEPTH']:
            qacomments['DEPTH'] = 'DEPTH invalid format'
    except KeyError:
        pass
    #
    # Wind direction may be a heading (e.g., NE, SW) or an azimuth.
    # Assume it's a heading; if not it'll throw an error
    #
    try:
        if rowdata['WDIR'] != '':
            try:
                rowdata['WDIR'] = compass2azimuth[rowdata['WDIR'].upper().strip()]
                qadata['WDIR'] = QA_CHANGED
                qacomments['WDIR'] = 'WDIR converted from compass heading to azimuth'
            except AttributeError, msg:
                try:
                    rowdata['WDIR'] = float(unsigned_number_regex.findall(rowdata['WDIR'])[0])
                    qadata['WDIR'] = QA_CORRECT
                except (TypeError, IndexError), msg:
                    rowdata['WDIR'] = ''
                    qadata['WDIR'] = QA_ERRONEOUS
                    qacomments['WDIR'] = 'WDIR unexpected data in input'
            except KeyError, msg:
                rowdata['WDIR'] = ''
                qadata['WDIR'] = QA_ERRONEOUS
                qacomments['WDIR'] = 'WDIR unexpected data in input'
    except KeyError:
        pass
    #
    # Wind speed
    # Sometimes will have extra formatting, including units
    # Sometimes expressed as knots vice MPH
    # Sometimes expressed as a range (e.g., '10-15 mph')
    #
    try:
        if rowdata['WSPD'] != '':
            try:
                rowdata['WSPD'] = float(rowdata['WSPD'])
                qadata['WSPD'] = QA_CORRECT
            except ValueError, msg:
                wspd_in_knots = rowdata['WSPD'].lower().find('knot') != -1 or \
                                rowdata['WSPD'].lower().find('kt') != -1
                if rowdata['WSPD'].lower() == 'calm':
                    rowdata['WSPD'] = 0.0
                    qadata['WSPD'] = QA_CHANGED
                    qacomments['WSPD'] = 'WSPD converted from "calm" to 0.0'
                elif rowdata['WSPD'].find('-') != -1 or rowdata['WSPD'].find('to') != -1:
                    #
                    # Windspeed expressed as a range (e.g., '10-15 mph' or '10 to 15 mph')
                    # Here we'll just split the difference.
                    #
                    min, max = (float(x) for x in unsigned_number_regex.findall(rowdata['WSPD']))
                    rowdata['WSPD'] = min + (max - min)/2.0
                    qadata['WSPD'] = QA_CHANGED
                    qacomments['WSPD'] = 'WSPD converted from range to median value'
                else:
                    try:
                        rowdata['WSPD'] = float(unsigned_number_regex.findall(rowdata['WSPD'])[0])
                        qadata['WSPD'] = QA_CORRECT
                    except IndexError:
                        rowdata['WSPD'] = ''
                        qadata['WSPD'] = QA_ERRONEOUS
                        qacomments['WSPD'] = 'WSPD unexpected data in input'
                if wspd_in_knots:
                    try:
                        rowdata['WSPD'] = rowdata['WSPD'] * KTS2MPH
                        qadata['WSPD'] = QA_CHANGED
                        qacomments['WSPD'] = 'WSPD converted from knots to MPH'
                    except TypeError:
                        pass
            except KeyError:
                pass
    except KeyError:
        pass
    #
    # Water temperature
    # Usually ints or floats, but may have the units accompanying; e.g., 29.4 [degree symbol]C,
    # in which case it will be a Unicode string.  Here we first attempt to cast it as a float;
    # if that fails, we can assume there's extra cruft in the value and so use a regex to extract
    #  the numerical quantity.
    #
    try:
        rowdata['WTEMP'] = float(rowdata['WTEMP'])
        qadata['WTEMP'] = QA_CORRECT
    except ValueError:
        try:
            rowdata['WTEMP'] = float(unsigned_number_regex.findall(rowdata['WTEMP'])[0])
        except IndexError:
            if rowdata['WTEMP']:
                qacomments['WTEMP'] = 'WTEMP unexpected data in input'
                qadata['WTEMP'] = QA_CHANGED
                rowdata['WTEMP'] = ''
    except KeyError:
        pass
    #
    # Some spreadsheets have water temps as a mix of celsius and Fahrenheit.
    # If the water temp value is greater than 33.0, assume it's Fahrenheit and convert.
    #
    try:
        if type(rowdata['WTEMP']) == types.FloatType:
            if rowdata['WTEMP'] > 33.0:
                rowdata['WTEMP'] = F2C(rowdata['WTEMP'])
                qadata['WTEMP'] = QA_CHANGED
                qacomments['WTEMP'] = 'WTEMP inferred as Fahrenheit based on value and converted to celsius'
    except KeyError:
        pass
    #
    # Salinity
    # Sometimes cells are formatted as text and not numeric, so cast to float just in case
    # Sometimes expressed as a range of top and bottom measurements (e.g., 'T6.6/B6.7')
    #   In this case use a regex to find the numerical parts, then use the first
    #   one found (presumably the surface measurement)
    #
    try:
        rowdata['SALINITY'] = float(rowdata['SALINITY'])
        qadata['SALINITY'] = QA_CORRECT
    except (TypeError, ValueError):
        try:
            rowdata['SALINITY'] = float(unsigned_number_regex.findall(rowdata['SALINITY'])[0])
            qadata['SALINITY'] = QA_CHANGED
            qacomments['SALINITY'] = 'SAL converted from original formatted value'
        except IndexError:
            rowdata.pop('SALINITY')
    except KeyError:
        pass
    #
    # Dissolved Oxygen
    # Sometimes cells are formatted as text and not numeric, so cast to float just in case
    # Sometimes expressed as a range of top and bottom measurements (e.g., 'T6.6/B6.7')
    #   In this case use a regex to find the numerical parts, then use the first
    #   one found (presumably the surface measurement)
    #
    """
    try:
        rowdata['DO'] = float(rowdata['DO'])
        qadata['DO'] = QA_CORRECT
    except (TypeError, ValueError):
        try:
            rowdata['DO'] = float(unsigned_number_regex.findall(rowdata['DO'])[0])
            qadata['DO'] = QA_CHANGED
            qacomments['DO'] = 'DO converted from original formatted value'
        except IndexError:
            rowdata.pop('DO')
    except KeyError:
        pass
    """
    #
    # Barometric Pressure
    # Sometimes cells are formatted as text and not numeric, so cast to float just in case
    # Sometimes expressed as a range of top and bottom measurements (e.g., 'T6.6/B6.7')
    #   In this case use a regex to find the numerical parts, then use the first
    #   one found (presumably the surface measurement)
    #
    """
    try:
        rowdata['PRES'] = float(rowdata['PRES'])
        qadata['PRES'] = QA_CORRECT
    except (TypeError, ValueError):
        try:
            rowdata['PRES'] = float(unsigned_number_regex.findall(rowdata['PRES'])[0])
            qadata['PRES'] = QA_CHANGED
            qacomments['PRES'] = 'PRES converted from original formatted value'
        except IndexError:
            rowdata.pop('PRES')
    except KeyError:
        pass
    """
    #
    # Check against valid ranges
    #
    logging.info('    Performing range checking...')
    for param in validRanges.keys():
        try:
            if not checkRange(rowdata[param], validRanges[param]):
                qadata[param] = QA_ERRONEOUS
                qacomments[param] = '%s value (%.2f) exceeds valid range %s'%(param, rowdata[param], validRanges[param])
        except (KeyError, TypeError):
            pass
