#---------------------------------------------------------------------------------------------
#
# ranges.py
#   Dictionary containing valid minima/maxima for certain environmental parameters.
#
#---------------------------------------------------------------------------------------------

validRanges = {
               'WSPD'      : (0.0, 40.25),    # wind speed, mph
               'WTEMP'     : (-2.0, 40.0),    # water temperature, deg. c
               'SALINITY'  : (0.0, 50.0),     # salinity, PPT
               'DEPTH'     : (0.0, 100.0),    # water depth, meters
               #'DO'        : (0.0, 650.0),    # dissolved oxygen, millimoles/m3
               'WDIR'      : (0.0, 360.0),    # wind direction, degrees of azimuth
               #'PRES'      : (850.0, 1030.0)  # barometric pressure, hPa
               'LATITUDE'  : (23.0, 32.0),
               'LONGITUDE' : (-98.0, -78.0)
              }

