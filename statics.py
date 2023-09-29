#---------------------------------------------------------------------------------------------
#
# statics.py
#  ORGANISM and BIN_LEVEL records used when running HABSGrabber in 'dry-run' mode.
#  These records simulate data retrieved from the SDE.
#
#---------------------------------------------------------------------------------------------

staticOrganisms = [{'FAMILY': 'Gymnodiniaceae', 'OBJECTID': 1, 'CLASS': 'Dinophyceae', 'ORDER_BIO': 'Gymnodiniales', 'GENUS': 'Karenia', 'SPECIES': 'brevis'},
                   {'FAMILY': 'Gymnodiniaceae', 'OBJECTID': 2, 'CLASS': 'Dinophyceae', 'ORDER_BIO': 'Gymnodiniales', 'GENUS': 'Gyrodinium', 'SPECIES': 'spirale'},
                   {'FAMILY': 'Gymnodiniaceae', 'OBJECTID': 3, 'CLASS': 'Dinophyceae', 'ORDER_BIO': 'Gymnodiniales', 'GENUS': 'Gyrodinium', 'SPECIES': 'sp.'},
                   {'FAMILY': 'Gymnodiniaceae', 'OBJECTID': 4, 'CLASS': 'Dinophyceae', 'ORDER_BIO': 'Gymnodiniales', 'GENUS': 'Gyrodinium', 'SPECIES': 'spp.'}]
staticBinLevels = [
                   {'OBJECTID': 29, 'MAX_VALUE': 1.0, 'REGION': 'Gulf of Mexico', 'MIN_VALUE': 0.0, 'ORGANISM_ID': 1, 'BIN_LEVEL': 'not observed'},
                   {'OBJECTID': 30, 'MAX_VALUE': 10000.0, 'REGION': 'Gulf of Mexico', 'MIN_VALUE': 1.0, 'ORGANISM_ID': 1, 'BIN_LEVEL': 'very low'},
                   {'OBJECTID': 31, 'MAX_VALUE': 100000.0, 'REGION': 'Gulf of Mexico', 'MIN_VALUE': 10000.0, 'ORGANISM_ID': 1, 'BIN_LEVEL': 'low'},
                   {'OBJECTID': 32, 'MAX_VALUE': 1000000.0, 'REGION': 'Gulf of Mexico', 'MIN_VALUE': 100000.0, 'ORGANISM_ID': 1, 'BIN_LEVEL': 'medium'},
                   {'OBJECTID': 33, 'MAX_VALUE': 10000000000.0, 'REGION': 'Gulf of Mexico', 'MIN_VALUE': 1000000.0, 'ORGANISM_ID': 1, 'BIN_LEVEL': 'high'},
                   {'OBJECTID': 9,  'MAX_VALUE': 1000.0, 'REGION': 'Gulf of Mexico', 'MIN_VALUE': 0.0, 'ORGANISM_ID': 2, 'BIN_LEVEL': 'not observed'},
                   {'OBJECTID': 12, 'MAX_VALUE': 10000.0, 'REGION': 'Gulf of Mexico', 'MIN_VALUE': 1000.0, 'ORGANISM_ID': 2, 'BIN_LEVEL': 'very low'},
                   {'OBJECTID': 15, 'MAX_VALUE': 100000.0, 'REGION': 'Gulf of Mexico', 'MIN_VALUE': 10000.0, 'ORGANISM_ID': 2, 'BIN_LEVEL': 'low'},
                   {'OBJECTID': 18, 'MAX_VALUE': 1000000.0, 'REGION': 'Gulf of Mexico', 'MIN_VALUE': 100000.0, 'ORGANISM_ID': 2, 'BIN_LEVEL': 'medium'},
                   {'OBJECTID': 21, 'MAX_VALUE': 10000000000.0, 'REGION': 'Gulf of Mexico', 'MIN_VALUE': 1000000.0, 'ORGANISM_ID': 2, 'BIN_LEVEL': 'high'},
                   {'OBJECTID': 10, 'MAX_VALUE': 1000.0, 'REGION': 'Gulf of Mexico', 'MIN_VALUE': 0.0, 'ORGANISM_ID': 3, 'BIN_LEVEL': 'not observed'},
                   {'OBJECTID': 13, 'MAX_VALUE': 10000.0, 'REGION': 'Gulf of Mexico', 'MIN_VALUE': 1000.0, 'ORGANISM_ID': 3, 'BIN_LEVEL': 'very low'},
                   {'OBJECTID': 16, 'MAX_VALUE': 100000.0, 'REGION': 'Gulf of Mexico', 'MIN_VALUE': 10000.0, 'ORGANISM_ID': 3, 'BIN_LEVEL': 'low'},
                   {'OBJECTID': 19, 'MAX_VALUE': 1000000.0, 'REGION': 'Gulf of Mexico', 'MIN_VALUE': 100000.0, 'ORGANISM_ID': 3, 'BIN_LEVEL': 'medium'},
                   {'OBJECTID': 22, 'MAX_VALUE': 10000000000.0, 'REGION': 'Gulf of Mexico', 'MIN_VALUE': 1000000.0, 'ORGANISM_ID': 3, 'BIN_LEVEL': 'high'},
                   {'OBJECTID': 11, 'MAX_VALUE': 1000.0, 'REGION': 'Gulf of Mexico', 'MIN_VALUE': 0.0, 'ORGANISM_ID': 4, 'BIN_LEVEL': 'not observed'},
                   {'OBJECTID': 14, 'MAX_VALUE': 10000.0, 'REGION': 'Gulf of Mexico', 'MIN_VALUE': 1000.0, 'ORGANISM_ID': 4, 'BIN_LEVEL': 'very low'},
                   {'OBJECTID': 17, 'MAX_VALUE': 100000.0, 'REGION': 'Gulf of Mexico', 'MIN_VALUE': 10000.0, 'ORGANISM_ID': 4, 'BIN_LEVEL': 'low'},
                   {'OBJECTID': 20, 'MAX_VALUE': 1000000.0, 'REGION': 'Gulf of Mexico', 'MIN_VALUE': 100000.0, 'ORGANISM_ID': 4, 'BIN_LEVEL': 'medium'},
                   {'OBJECTID': 23, 'MAX_VALUE': 10000000000.0, 'REGION': 'Gulf of Mexico', 'MIN_VALUE': 1000000.0, 'ORGANISM_ID': 4, 'BIN_LEVEL': 'high'}]

