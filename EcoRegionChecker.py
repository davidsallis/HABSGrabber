from EcoRegionDefines import *
from EcoRegionSubsystems import *
import math

def isInGeoPoly(test_point,coords):
    """This method checks test point, which is a (lon,lat) tuple against
    a polygon defined by a list of co-ordinates (coords) which consists of
    (lon,lat) tuples.  These tuples could contain a 'z', but it will be ignored"""

    #the number of line segments is one less than the number of points
    curpoint=0
    intersect_count=0 #this will be used to count how many lines intersect a ray going south from the test point
        #if the count is odd, the point is inside the polygon
    point_count=len(coords)
    while curpoint<point_count-1:#don't use the last point because it's the end segment to the penultimate point
        segment=(coords[curpoint],coords[curpoint+1])#make a line segment to check
        if checkLineHitsSouthRay(test_point,segment):#if the ray hits increment the intersect counter
            intersect_count+=1
        curpoint+=1
    #
    # return True if the number of intersections is odd, False if not
    #
    return intersect_count%2 == 1
    
def checkLineHitsSouthRay(test_point,test_line):
    """This checks whether a line, defined by test_line,
    intersects a vertical ray that points towards -y (south) with test_point as its origin"""

    #make some quickie locals that are less cumbersome than the tuple components
    tl0_x, tl0_y = test_line[0]
    tl1_x, tl1_y = test_line[1]
    test_x, test_y = test_point

    #ignore vertical lines: if the point is right over the vertical line, it 
    #will intersect one of the other lines that connects
    #to it anyway,
    if tl0_x == tl1_x:
        return False
    
    #check to see if the both of the endpoints have a y above our test point; 
    #if so, it can't intersect, so return False
    if tl0_y > test_y and tl1_y > test_y:
        return False

    #get the distance between the x's, it will be used for interpolation later, but now it will be used to check for lines that span
    #the date line.  Any line that is more than 90 degrees wide in longitude is suspect
    delta_x=tl1_x-tl0_x
    if math.fabs(delta_x)>90.0:
        #assume the test point has been normalized to be between -180 and 180, if this is the case, the two endpoint lons should match
        #the sign of the test point to make the math work correctly
        if test_x>0.0:
            if tl0_x<0.0:
                tl0_x+=360
            if tl1_x<0.0:
                tl1_x+=360
        else: #otherwise, subtract 360 from any positives
            if tl0_x>0.0:
                tl0_x-=360
            if tl1_x>0.0:
                tl1_x-=360
        #now that the tl*x points have been shuffled, re-compute the delta    
        delta_x=tl1_x-tl0_x
    
    #check to see if both endpoints are on the same side of the line; if so, it can't intersect
    if(tl0_x>=test_x and tl1_x>=test_x) or (tl0_x<test_x and tl1_x<test_x):
        return False

    #check to see if the both of the endpoints have a y below our test point or below the bottom, if so, it may intersect
    #but we can do a simpler intersection test
    if tl0_y <= test_y and tl1_y <= test_y:
        #if the endpoints are on different sides, the lines intersect, but the possibility of the points being on the
        #same side has been eliminated already so return True
        return True

    #find the interesection and make sure its 'y' is less than test_y and greater than bottom
    delta_y=tl1_y-tl0_y
    slope=delta_y/delta_x
    inter_y=slope*(test_x-tl0_x)+tl0_y
    if inter_y<test_y:
        return True #the intersection is south of the test point, so return true
    else:
        return False

def findEcoRegionForCoords(inlon, inlat):
    """This method returns a tuple containing the ecosystems and ecoregions
    to which a coordinate belongs.  If the point isn't in any regions, the
    region list will be the empty list; if it's in no system, the system list
    will be the empty list"""
    
    inpoint = (inlon,inlat)
    #
    # the ecosystems and ecoregions are in the same list, but there is an index
    # dictionary that connects regions to systems, so this method will iterate
    # through all systems, and for systems that the point it in (There is usually
    # only one, if any) all sub-regions of that system will be checked.
    #
    systems = [] # this is the list of ecosystems the point is in
    regions = [] # this is the list of regions the point is in
    for sys_key in eco_sub_systems:
        # the key is the name of the region, so get its polygon(s) and see if
        # the point lies within.
        for system_polygon in eco_region_polygons[sys_key]:
            if isInGeoPoly(inpoint, system_polygon):
                # the point is in the system...
                systems.append(sys_key)
                # now check the sub-regions
                for subsys_key in eco_sub_systems[sys_key]:
                    for region_polygon in eco_region_polygons[subsys_key]: 
                        if isInGeoPoly(inpoint,region_polygon):
                            regions.append(subsys_key)
                            break
                break
            
    systems.sort()
    regions.sort()
    return(systems,regions)
        
def doGulfScanTest():
    gulf_trap=eco_region_polygons['Gulf of Mexico']
    #create an ascii-art grid where '##' appears for points that are in the polygon and '++' appears for ones that aren't
    asciiart=[]#make a buffer for the big picture
    for curlat in range(35,20,-1):
        stringer=[]#make a buffer for one line
        for curlon in range(-100,-80,1):
            ecosys,ecoreg=findEcoRegionForCoords(float(curlon),float(curlat))
            #in_eco=isInGeoPoly((float(curlon),float(curlat)),gulf_trap)
            if len(ecosys)>0:
                stringer.append(ecosys[0][0])#use the first character of the name
            else:
                stringer.append('+')
            if len(ecoreg)>0:
                stringer.append(ecoreg[0][0])#use the first character of the name
            else:
                stringer.append('-')
            
        scanline="".join(stringer)#combine the array of strings into one long string for a scan-line
        asciiart.append(scanline)#add it to the output ascii image
    print "Polygon ASCII art: \n"+"\n".join(asciiart) #join the image so that each scanline is one line of text
            
