#---------------------------------------------------------------------------------------------
#
# constants.py
#   Constant values used in HABS data processing
#
#---------------------------------------------------------------------------------------------
SECS_IN_DAY = 86400.0   # Used to convert Excel timestamps to Unix epoch
#
# Unit conversion
#
F2C = lambda x: (x - 32.0 ) * (5.0 / 9.0)    # Fahrenheit to Celsius
C2F = lambda x: (9.0 / 5.0) * x + 32.0       # Celsius to Fahrenheit
F2M = .3048                                  # feet to meters
KTS2MPH = 1.1507794                          # knots to miles per hour
MPH2KTS = 0.8689762                          # miles per hour to knots
#
# QA flags
#
QA_NOT_ASSIGNED = 0
QA_CORRECT = 1
QA_PROBABLY_GOOD = 2
QA_DOUBTFUL = 3
QA_ERRONEOUS = 4
QA_CHANGED = 5
QA_MISSING = 9
#
# Convert compass headings (N, NE, E, etc.) to azimuth values
#
compass2azimuth = {'N':360.0,
                   'NORTH':360.0,
                   'NNE':22.5,
                   'NNORTHEAST':22.5,
                   'NE':45.0,
                   'NORTHEAST':45.0,
                   'ENE':67.5,
                   'ENORTHEAST':67.5,
                   'E':90.0,
                   'EAST':90.0,
                   'ESE':112.5,
                   'ESOUTHEAST':112.5,
                   'SE':135.0,
                   'SOUTHEAST':135.0,
                   'SSE':157.5,
                   'SSOUTHEAST':157.5,
                   'S':180.0,
                   'SOUTH':180.0,
                   'SSW':202.5,
                   'SSOUTHWEST':202.5,
                   'SW':225.0,
                   'SOUTHWEST':225.0,
                   'WSW':247.5,
                   'WSOUTHWEST':247.5,
                   'W':270.0,
                   'WEST':270.0,
                   'WNW':292.5,
                   'WNORTHWEST':292.5,
                   'NW':315.0,
                   'NORTHWEST':315.0,
                   'NNW':337.5,
                   'NNORTHWEST':337.5}
#
# Map spreadsheet column headers to database column names
#
termMap = {'TX': {
                  'Agency':'AGENCY',
                  'Barometric Pressure':'PRES',
                  'Air Temperature':'ATMP',
                  'Collectors':'COLLECTOR',
                  'Date':'DATE',
                  'Field Wind Direction':'WDIR2',
                  'Field Wind Speed (mph)':'WSPD2',
                  'Karenia cells/mL':'CELLCOUNT',
                  'N Latitude':'LATITUDE',
                  'Notes':'COMMENTS',
                  'Salinity':'SALINITY',
                  'Salinity (ppt)':'SALINITY',
                  'SPI TCOON Wind Speed (mph)':'WSPD1',
                  'SPI TCOON Wind Direction':'WDIR1',
                  'Station Description':'DESCRIPTION',
                  'Time':'TIME',
                  'Time (GMT) CST+5':'TIME',
                  'W Longitude':'LONGITUDE',
                  #'Water Color':'WATER COLOR',
                  'Water Depth (ft)':'DEPTH',
                  'Total Water Depth (ft)':'DEPTH',
                  'Water Temperature (oC)':'WTEMP',
                  'Water Temperature':'WTEMP',
                  'Wind Speed':'WSPD',
                  'Wind Direction':'WDIR',
                  'Dissolved Oxygen':'DO',
                  'Organism Id':'ORGANISM_ID',
                  'Dummy categories':'CATEGORIES',
                  'Dummy cellcounts':'CELLCOUNTS',
                  'Dummy organism ids':'ORGANISM_IDS',
                  'Dummy WSPD':'WSPD',
                  'Dummy WDIR':'WDIR',
                  'STATE':'STATE',
                  'ECOREGION':'ECOREGION',
                  'ECOSYSTEM':'ECOSYSTEM',
                 },
           'AL': {
                  'Agency':'AGENCY',
                  'Barometric Pressure':'PRES',
                  'Air Temperature':'ATMP',
                  'Collectors':'COLLECTOR',
                  'Date':'DATE',
                  'Field Wind Direction':'WDIR2',
                  'Field Wind Speed (mph)':'WSPD2',
                  'Karenia cells/L':'CELLCOUNT',
                  'N Latitude':'LATITUDE',
                  'Notes':'COMMENTS',
                  'Salinity':'SALINITY',
                  'Salinity (ppt)':'SALINITY',
                  'Station Description':'DESCRIPTION',
                  'Time':'TIME',
                  'Time (GMT) CST+5':'TIME',
                  'W Longitude':'LONGITUDE',
                  #'Water Color':'WATER COLOR',
                  'Water Depth (ft)':'DEPTH',
                  'Total Water Depth (ft)':'DEPTH',
                  'Water Temperature (oC)':'WTEMP',
                  'Water Temperature':'WTEMP',
                  'Wind Speed':'WSPD',
                  'Wind Direction':'WDIR',
                  'Dissolved Oxygen':'DO',
                  'Organism Id':'ORGANISM_ID',
                  'Dummy categories':'CATEGORIES',
                  'Dummy cellcounts':'CELLCOUNTS',
                  'Dummy organism ids':'ORGANISM_IDS',
                  'Dummy WSPD':'WSPD',
                  'Dummy WDIR':'WDIR',
                  'STATE':'STATE',
                  'ECOREGION':'ECOREGION',
                  'ECOSYSTEM':'ECOSYSTEM',
                 },
           'MS': {
                  'Agency':'AGENCY',
                  'AL Lab Number':'HABSOS_ID',
                  'Barometric Pressure':'PRES',
                  'Air Temperature':'ATMP',
                  'Air Temp':'ATMP',
                  'Air Temperature \xb0C':'ATMP',
                  'Collectors':'COLLECTOR',
                  'Collected By':'COLLECTOR',
                  'Date':'DATE',
                  'Date Collected':'DATE',
                  'Field Wind Direction':'WDIR2',
                  'Field Wind Speed (mph)':'WSPD2',
                  'Karenia cells/mL':'CELLCOUNT',
                  'Karenia_brevis':'CELLCOUNT',
                  'Cell/L Count':'CELLCOUNT',
                  'N Latitude':'LATITUDE',
                  'Latitude':'LATITUDE',
                  'Lat DD':'LATITUDE',
                  'Notes':'COMMENTS',
                  'Salinity':'SALINITY',
                  'Salinity (ppt)':'SALINITY',
                  'S. Sal (ppt)':'SALINITY',
                  'Sample Location':'DESCRIPTION',
                  'SPI TCOON Wind Speed (mph)':'WSPD1',
                  'SPI TCOON Wind Direction':'WDIR1',
                  'Wind Direction':'WDIR1',
                  'Station Description':'DESCRIPTION',
                  'Water Sample Location':'DESCRIPTION',
                  'Time':'TIME',
                  'Time Collected':'TIME',
                  'W Longitude':'LONGITUDE',
                  'Longitude':'LONGITUDE',
                  'Lon DD':'LONGITUDE',
                  #'Water Color':'WATER COLOR',
                  'Water Depth (ft)':'DEPTH',
                  'Water Temperature (oC)':'WTEMP',
                  'Water Temperature (\xb0C)':'WTEMP',
                  'Water Temperature \xb0C':'WTEMP',
                  'Water Temperature':'WTEMP',
                  'Sample Temperature \xb0C':'WTEMP',
                  'Sample Temperature':'WTEMP',
                  'S. Temp (C)':'WTEMP',
                  'Wind Speed':'WSPD',
                  'Wind Speed (knots)':'WSPD',
                  'Wind Speed MPH':'WSPD',
                  'Wind Direction':'WDIR',
                  'Dissolved Oxygen':'DO',
                  'S. DO (ppm)':'DO',
                  'DO mg/L':'DO',
                  'Organism Id':'ORGANISM_ID',
                  'Dummy categories':'CATEGORIES',
                  'Dummy cellcounts':'CELLCOUNTS',
                  'Dummy organism ids':'ORGANISM_IDS',
                  'Dummy WSPD':'WSPD',
                  'Dummy WDIR':'WDIR',
                  'STATE':'STATE',
                  'ECOREGION':'ECOREGION',
                  'ECOSYSTEM':'ECOSYSTEM',
                 },
           'FL': {
                  'Dummy cellcounts':'CELLCOUNTS',
                  'Dummy organism ids':'ORGANISM_IDS',
                  'Dummy categories':'CATEGORIES',
                  'Dummy qacomments':'QA_COMMENT',
                  'PROOF_DATE':'PROOF_DATE',
                  'COMMENT':'COMMENTS',
                  'SAMPLE_DATE':'DATE',
                  'SAMPLE_TIME':'TIME',
                  'HAB_ID':'HABSOS_ID',
                  'Dummy description':'DESCRIPTION',
                  'LATITUDE':'LATITUDE',
                  'LONGITUDE':'LONGITUDE',
                  'DEPTH':'DEPTH',
                  'SALINITY':'SALINITY',
                  'DISSOLVED_O2':'DO',
                  'WIND_SPEED':'WSPD',
                  'WIND_DIR':'WDIR',
                  'WDIR':'WDIR',
                  'TEMP':'WTEMP',
                  'WTEMP':'WTEMP',
                  'AIR_TEMP':'ATMP',
                  'K_brevis':'K_brevis',
                  'K-brevis':'K_brevis',
                  'K.brevis':'K_brevis',
                  'K. brevis':'K_brevis',
                  'STATE':'STATE',
                  'AGENCY':'AGENCY',
                  'ECOREGION':'ECOREGION',
                  'ECOSYSTEM':'ECOSYSTEM',
                 },
           }
#
# Units for observed parameters
#
unitMap = {'DEPTH':'m',
           'CELLCOUNTS':{
                         'Karenia brevis':'cells/L',
                         'Gyrodinium sp.':'cells/L',
                         'Gyrodinium spp.':'cells/L',
                         'Gyrodinium spirale':'cells/L',
                        },
           'WDIR':'deg',
           'WSPD':'mph',
           'SALINITY':'ppt',
           'WTEMP':'deg. C',
           'ATMP':'deg. C',
           'DO':'ppm',
           'PRES':'hPa',
          } 
#
# Map agency abbreviations to fully qualified address info
#
agencyMap = {
             'City of SPI':{'institution':'City of South Padre Island',
                          'address':'4601 Padre Blvd',
                          'city':'South Padre Island',
                          'state':'Texas',
                          'zipcode':'78597'},
             'PINS'      :{'institution':'Padre Island National Seashore',
                          'address':'20420 Park Road 22',
                          'city':'Corpus Christi',
                          'state':'Texas',
                          'zipcode':'78418'},
             'UTMSI'    :{'institution':'University of Texas Marine Science Institute',
                          'address':'1201 W University Drive',
                          'city':'Edinburg',
                          'state':'Texas',
                          'zipcode':'78539'},
             'DSHS'     :{'institution':'Texas Department of State Health Services',
                          'address':'1100 West 49th Street',
                          'city':'Austin',
                          'state':'Texas',
                          'zipcode':'78756'},
             'TAEX'     :{'institution':'Texas Agricultural Extension Service',
                          'address':'600 John Kimbrough Boulevard, Suite 509',
                          'city':'College Station',
                          'state':'Texas',
                          'zipcode':'77843'},
             'TPWD'     :{'institution':'Texas Parks & Wildlife Department',
                          'address':'4200 Smith School Road',
                          'city':'Austin',
                          'state':'Texas',
                          'zipcode':'78744'},
             'UT-Pan Am':{'institution':'University of Texas-Pan American',
                          'address':'1201 W University Drive',
                          'city':'Edinburg',
                          'state':'Texas',
                          'zipcode':'78539'},
             'UTPA'     :{'institution':'University of Texas-Pan American',
                          'address':'1201 W University Drive',
                          'city':'Edinburg',
                          'state':'Texas',
                          'zipcode':'78539'},
             'MSDMR'    :{'institution':'Mississippi Department of Marine Resources',
                          'address':'1141 Bayview Avenue',
                          'city':'Biloxi',
                          'state':'Mississippi',
                          'zipcode':'39530'},
             'FWC'      :{'institution':'Florida Fish and Wildlife Conservation Commission',
                          'address':'100 8th Ave. SE',
                          'city':'St. Petersburg',
                          'state':'Florida',
                          'zipcode':'33701'},
           }
#
# MS DMR spreadsheets ID collectors by their initials, so we map those to
# full names here
#
MSDMRCollectors = {
                   'KB':'Kristina Broussard',
                   'SB':'Steve Breland',
                   'JM':'John Mitchell',
                  }

ORGANISM = {'class':'Dinophyceae',
            'family':'Gymnodiniaceae',
            'genus':'Karenia',
            'order_bio':'Gymnodiniales',
            'species':'brevis'}
#
# Time/date format strings
#
dateFormats = {
               '%Y'                      : '%Y',
               '%Y0000'                  : '%Y',
               '%Y%m00'                  : '%Y',
               '%Y%m'                    : '%Y-%m',
               '%Y%m%d'                  : '%Y-%m-%d',
               '%Y%m%d %H%M'             : '%Y-%m-%d',
               '%Y%m%d %I%M %p'          : '%Y-%m-%d',
               '%Y%m%d %I%M%p'           : '%Y-%m-%d',
               '%Y%m%d %H%M%S'           : '%Y-%m-%d',
               '%Y%m%d %I%M%S %p'        : '%Y-%m-%d',
               '%Y%m%d %I%M%S%p'         : '%Y-%m-%d',
               '%Y%m%d %H:%M'            : '%Y-%m-%d',
               '%Y%m%d %I:%M %p'         : '%Y-%m-%d',
               '%Y%m%d %I:%M%p'          : '%Y-%m-%d',
               '%Y%m%d %H:%M:%S'         : '%Y-%m-%d',
               '%Y%m%d %I:%M:%S %p'      : '%Y-%m-%d',
               '%Y%m%d %I:%M:%S%p'       : '%Y-%m-%d',
               '%m/%Y'                   : '%Y-%m',
               '%m/%d/%Y'                : '%Y-%m-%d',
               '%m/%d/%Y %H%M'           : '%Y-%m-%d',
               '%m/%d/%Y %I%M %p'        : '%Y-%m-%d',
               '%m/%d/%Y %I%M%p'         : '%Y-%m-%d',
               '%m/%d/%y %H%M%S'         : '%Y-%m-%d',
               '%m/%d/%y %I%M%S %p'      : '%Y-%m-%d',
               '%m/%d/%y %I%M%S%p'       : '%Y-%m-%d',
               '%m/%d/%Y %H:%M'          : '%Y-%m-%d',
               '%m/%d/%Y %I:%M %p'       : '%Y-%m-%d',
               '%m/%d/%Y %I:%M%p'        : '%Y-%m-%d',
               '%m/%d/%Y %H:%M:%S'       : '%Y-%m-%d',
               '%m/%d/%Y %I:%M:%S %p'    : '%Y-%m-%d',
               '%m/%d/%Y %I:%M:%S%p'     : '%Y-%m-%d',
               '%m/%d/%Y %H:%M:%S.%f'    : '%Y-%m-%d',
               '%m/%d/%Y %H:%M:'         : '%Y-%m-%d',
               '%m/%d/%Y %H:%M: %p'      : '%Y-%m-%d',
               '%m/%d/%Y %H:%M:%p'       : '%Y-%m-%d',
               '%Y-%m'                   : '%Y-%m',
               '%Y-%m-%d'                : '%Y-%m-%d',
               '%Y-%m-%d %H%M'           : '%Y-%m-%d',
               '%Y-%m-%d %I%M %p'        : '%Y-%m-%d',
               '%Y-%m-%d %H:%M'          : '%Y-%m-%d',
               '%Y-%m-%d %I:%M %p'       : '%Y-%m-%d',
               '%Y-%m-%d %H:%M:%S'       : '%Y-%m-%d',
               '%Y-%m-%d %H:%M:%S 5p'    : '%Y-%m-%d',
               '%Y-%m-%d %H:%M:%S.%f'    : '%Y-%m-%d',
               '%Y-%m-%d %H:%M:%S.%f %p' : '%Y-%m-%d',
               '%b %Y'                   : '%Y-%m',
               '%b-%Y'                   : '%Y-%m',
               '%d %b %Y'                : '%Y-%m-%d',
               '%d-%b-%Y'                : '%Y-%m-%d',
               '%d/%b/%Y'                : '%Y-%m-%d',
               '%Y-%m-%dT%H:%M'          : '%Y-%m-%d',
               '%Y-%m-%dT%H:%M:%S'       : '%Y-%m-%d',
               '%Y-%m-%dT%H:%M:%SZ'      : '%Y-%m-%d',
               '%Y-%m-%dT%H:%M:%S Z'     : '%Y-%m-%d',
               '%d-%b-%y'                : '%Y-%m-%d',
               '%d-%b-%y %H%M'           : '%Y-%m-%d',
               '%d-%b-%y %H:%M'          : '%Y-%m-%d',
               '%b-%y'                   : '%Y-%m',
               '%d-%m-%y'                : '%Y-%m-%d',
               '%d-%m-%Y'                : '%Y-%m-%d',
               '%d/%m/%y'                : '%Y-%m-%d',
               '%d/%m/%Y'                : '%Y-%m-%d',
               }

timeFormats = {
               '%H:%M:%S'               : '%H:%M:%S',
               '%H:%M'                  : '%H:%M',
               '%H'                     : '%H',
               '%H%M%S'                 : '%H:%M:%S',
               '%H%M'                   : '%H:%M',
               '%H'                     : '%H:%M',
               '%I:%M:%S %p'            : '%H:%M:%S',
               '%I:%M:%S%p'             : '%H:%M:%S',
               '%I:%M %p'               : '%H:%M',
               '%I:%M%p'                : '%H:%M',
               '%I %p'                  : '%H',
               '%I%p'                   : '%H',
               '%I%M%S %p'              : '%H:%M:%S',
               '%I%M%S%p'               : '%H:%M:%S',
               '%I%M %p'                : '%H:%M',
               '%I%M%p'                 : '%H:%M',
               '%I%p'                : '%H:%M',
              }
