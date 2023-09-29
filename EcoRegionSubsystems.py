#
# Data structure describing which ecoregions are part of particular ecosystems
# N.B.:  Keys and values must agree with what is in EcoRegionDefines.py
#
eco_sub_systems = \
{   
 'alaska'            : [
                        'aleutian',
                        'beaufort sea',
                        'chukchi sea',
                        'east bering sea',
                        'gulf of alaska'
                       ],
 'atlantic basin'    : [],
 'california current': [
                        'north american pacific fjordland',
                        'north california',
                        'oregon',
                        'puget sound',
                        'south california bight',
                       ],
 'caribbean sea'     : [
                        'bahamian',
                        'east caribbean',
                        'greater antilles',
                        'southwest caribbean',
                        'west caribbean',
                        'south caribbean',
                       ],
 'east pacific basin': [],
 'great lakes'       : [],
 'gulf of mexico'    : [
                        'floridian', 
                        'north gulf of mexico',
                        'south gulf of mexico'
                       ],
#
# should be
# 'pacific islands'  : ['hawaii','guam','marianas'...]
#
 'pacific islands'   : ['hawaii'],
 'northeast shelf'   : [
                        'virginian',
                        'gulf of maine'
                       ],
 'southeast shelf'   : ['carolinian'],
}
