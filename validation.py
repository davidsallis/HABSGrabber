#-----------------------------------------------------------------------------------------
#
# Data validation routines
#
#-----------------------------------------------------------------------------------------
def checkRange(val, range):
    """
    """
    if val:
        return (val >= range[0]) and (val <= range[1])
    else:
        return True
