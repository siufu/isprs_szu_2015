################################
#author: Xiaohu
#created: 2015.7.18
#modified: 2015.8.5
################################

import csv
import os
import sys
#import pickle
import datetime
import pyproj
import json
import rtree
import math
from math import sqrt
from math import atan2
from sklearn.cluster import KMeans
from sklearn.cluster import MiniBatchKMeans
from sklearn.cluster import DBSCAN
from sklearn.preprocessing import StandardScaler
import numpy as np
import networkx as nx
from itertools import groupby
from bisect import bisect_left
import glob 
#import threading
import multiprocessing 
from collections import Counter



#switch between local and server
inServer = False 


if inServer:
    gpsFileName = '/home/public/data/GPS_DATA.csv'
    busFileName = '/home/public/data/BUS_ROUTE_DIC.csv'
    afcFileName = '/home/public/data/AFC_DATA.csv'
    geoFileName = '/home/public/data/ROAD_NETWORK.geojson'
else:
    busFileName = '../sample/BUS_ROUTE_DIC.csv'
    gpsFileName = '../sample/GPS_DATA_SAMPLE.csv'
    afcFileName = '../sample/AFC_DATA_SAMPLE.csv'
    geoFileName = '../sample/ROAD_NETWORK_SAMPLE.geojson'
    
testOn = False


class Point:
    """ Point class represents and manipulates x,y coords. """
    def __init__(self,x,y):
        """ Create a new point at the origin """
        self.x = x
        self.y = y



#---------------------------preprocessing---------------------------------
def ProjectCoors(lonlats, backward=False):
    #input: [[x1,y1],[x2,y2]...]
    #output: [[x1,y1],[x2,y2]...]
    #lon,lat -> x,y
    #x,y => lon,lat
    UTM49N = pyproj.Proj("+init=EPSG:32649")
    projected = []
    for ll in lonlats:
        x,y = UTM49N(ll[0],ll[1],inverse=backward)
        projected.append([x,y])
    return projected

def GeoJsonToGraph(geoFileName):
    #convert geojson into nodes and edges
    #node: id,x,y
    #edge: id,start,end
    
    nodes = []
    edges = []

    idx = rtree.index.Index()
    geoJson = json.load(open(geoFileName,'r'))
    for segment in geoJson['features']:
        roadid = int(segment['properties']['roadid'])
        direction = int(segment['properties']['direction'])
        
        if len(segment['geometry']['coordinates'])>1:
            print 'MultiLineString at:'+str(roadid)
            #sys.exit(0)

        
        for points in segment['geometry']['coordinates']:
        
            if direction==3:
                points = list(reversed(points))

            num_points = len(points)
            if direction == 2 or direction == 3:
                for i in range(0,num_points):
                    pointID = roadid*1000+i
                    x = points[i][0]
                    y = points[i][1]
                    bounds = (x-1e-14,y-1e-14,x+1e-14,y+1e-14)


                    #add Nodes
                    if i == 0 or i == num_points-1:
                        #check in rtree
                        
                        match = list(idx.intersection(bounds))
                        if len(match)>1:
                            print 'unexpected More Matches'
                            sys.exit(0)
                        elif len(match)==1:
                            pointID = match[0]
                        else:
                            idx.insert(pointID,bounds)
                            nodes.append([pointID,x,y])
                        
                    else:
                        #just add new point
                        #idx.insert(pointID,bounds)
                        nodes.append([pointID,x,y])

                    
                    #add Edges
                    if i==0:
                        lastID = pointID
                        continue
                    edgeID = roadid*1000+i
                    edges.append([edgeID,lastID,pointID])
                    lastID = pointID
            elif direction == 0 or direction == 1:
                #forward-----
                for i in range(0,num_points):
                    pointID = roadid*1000+i
                    x = points[i][0]
                    y = points[i][1]
                    bounds = (x-1e-14,y-1e-14,x+1e-14,y+1e-14)


                    #add Nodes
                    if i == 0 or i == num_points-1:
                        #check in rtree
                        
                        match = list(idx.intersection(bounds))
                        if len(match)>1:
                            print 'unexpected More Matches'
                            sys.exit(0)
                        elif len(match)==1:
                            pointID = match[0]
                        else:
                            idx.insert(pointID,bounds)
                            nodes.append([pointID,x,y])
                        
                    else:
                        #just add new point
                        #idx.insert(pointID,bounds)
                        nodes.append([pointID,x,y])

                    
                    #add Edges
                    if i==0:
                        lastID = pointID
                        continue
                    edgeID = roadid*1000+i*2-1
                    edges.append([edgeID,lastID,pointID])
                    edgeID = roadid*1000+i*2
                    edges.append([edgeID,pointID,lastID])
                    lastID = pointID
            else:
                print "unexpected direction:" +  str(direction)
        
    return nodes,edges

def ProjectNodes(nodes):
    #convert nodes from wgs84 to utm49
    UTM49N = pyproj.Proj("+init=EPSG:32649")
    
    nodesPro = []
    for item in nodes:
        x,y = UTM49N(item[1],item[2])
        item[1] = x
        item[2] = y
        nodesPro.append(item)
    return nodesPro
        
def GetNodesBoundingBox(nodeFileName):
    nodes = ReadNodes(nodeFileName)
    points = [value for key, value in nodes.iteritems()]
    boundbox = GetBoundingBox(points)
    fileName = './road/boundbox.txt'
    OutputLists(boundbox,fileName)
    return True
    
def ReadBoundingBox():
    fileName = './road/boundbox.txt'
    boundbox = []
    with open(fileName, 'rb') as f:
        for line in f:
            tmpList = [float(x) for x in line.split(',')]
            boundbox.append(tmpList)
    return boundbox

def OutputLists(li,fileName):
    #input: [[x11,x12,x13,x14],[x21,x22,x23,x24],.....]
    #output list to file
    
    with open(fileName, 'w') as f:
        for item in li:
            f.write(','.join([str(x)for x in item])+'\n')
    
def OutputSimpleList(li,fileName):
    #input:[x1,x2,.....]
    #output list to file
    with open(fileName, 'w') as f:
        for item in li:
            f.write(str(item)+'\n')

def OutputDisPathCount(disPathOD,afcCountOD,fileName):
    
    with open(fileName, 'w') as f:
        for i in range(len(afcCountOD)):
            item = disPathOD[i]
            f.write(','.join([str(x)for x in item])+','+str(afcCountOD[i])+'\n')

def ReOrganizeGPS(gpsFileName, busFileName, testOn):
    #output: bus/bus_id.file
    
    print "Reorganizing GPS files..."
    
    if testOn:
        print datetime.datetime.now()
    if not os.path.exists('./bus/'):
        os.makedirs('./bus/')
    busIds = []
    with open(busFileName,'rb') as busFile:
        busFile.readline()
        for line in busFile:
            busIds.append(int(line.split(',')[0]))
    print busIds

    allGPS = []                
    with open(gpsFileName,'rb') as gpsFile:
            gpsFile.readline()
            for line in gpsFile:
                tmpList = line.split(',')
                allGPS.append([int(tmpList[0]),int(tmpList[1]),int(tmpList[2]),
                               float(tmpList[3]),float(tmpList[4])])
    
    for busid in busIds:
        
        busIDFileName = './bus/bus_id'+str(busid)+'.txt'

        if os.path.isfile(busIDFileName):
            continue
        print busid
        
        gpsData = []
        for item in allGPS:
            if (busid == item[0]):
                gpsData.append([item[1],item[2],
                                item[3],item[4]])
            
        index = [x[0]*1000000+x[1] for x in gpsData]
        #sort gpsData
        sortGPSData = [x for (y,x) in sorted(zip(index,gpsData))]

        
        with open(busIDFileName, 'w') as busIDFile:
            for item in sortGPSData:
                busIDFile.write(','.join([str(x)for x in item])+'\n')
##        busIDDumpName = './bus/bus_id'+str(busid)
##        with open(busIDDumpName, 'wb') as busIDDump:
##            pickle.dump(sortGPSData, busIDDump)
        
    
    if testOn:
        print datetime.datetime.now()

    return True

def CleanBUSGPS(busFileName):
    print 'Running CleanGPS'
    busIds = []
    with open(busFileName,'rb') as busFile:
        busFile.readline()
        for line in busFile:
            busIds.append(int(line.split(',')[0]))
    print busIds
    
    for busid in busIds:
        busIDCleanFileName = './bus/bus_id_clean'+str(busid)+'.txt'
        if os.path.isfile(busIDCleanFileName):
            continue
        print busid
        
        gpsDataPro = CleanNProjectGPS(busid)
        
        OutputLists(gpsDataPro,busIDCleanFileName)

    return True

def CleanNProjectGPS(busid):
    #convert gpsData from wgs84 to utm49
    UTM49N = pyproj.Proj("+init=EPSG:32649")
    gpsData = GetGPS(busid)
    xmin = 113.5
    xmax = 114.5
    ymin = 22.2
    ymax = 23

    lastDay = 0
    gpsDataPro = []
    
    for item in gpsData:
        day = item[0]; time = item[1]
        lon = item[2]; lat = item[3]
        if lon<xmin or lon>xmax or lat<ymin or lat>ymax:
            continue
        
        x,y = UTM49N(lon,lat)
        item[2] = x
        item[3] = y
        
        if day == lastDay: #same day, diff time
            if time==lastTime:
                continue
            x,y = UTM49N(lon,lat)
            
            speed = Magnitude(Point(x,y),Point(lastX,lastY))/(time-lastTime)
            if speed>30:
                continue
        
        gpsDataPro.append(item)
        lastTime = time
        lastDay = day
        lastX = x
        lastY = y
    return gpsDataPro

#-------------------------------------------------------------------------

#-----------------------Get origins and destinations of a route----------------

def GetRouteIDs(busFileName):
    #get routes as dict
    #routeDict:{routeid:[bus1,2,3,...n]
    #routeDict[routeid]:
    
    routeDict = {}
        
    with open(busFileName,'rb') as busFile:
        busFile.readline()
        for line in busFile:
            bus_route = [int(x) for x in line.split(',')]
            if routeDict.has_key(bus_route[1]):
                routeDict[bus_route[1]].append(bus_route[0])
            else:
                routeDict[bus_route[1]] = [bus_route[0]]
    
    return routeDict



def GetGPS(busID):
    #get gps data of busid
    #return gpsData:[[day,time,lon,lat]...]
    busIDFileName = './bus/bus_id'+str(busID)+'.txt'
    gpsData = []
    
    with open(busIDFileName, 'rb') as busIDFile:
            for line in busIDFile:
                tmpList = line.split(',')
                gpsData.append([int(tmpList[0]),int(tmpList[1]),
                                    float(tmpList[2]),float(tmpList[3])])
    
                
    return gpsData

def GetProjectedGPS(busID):
    #get projected gps data of busid
    #return gpsData:[[day,time,x,y]...]
    busIDFileName = './bus/bus_id_clean'+str(busID)+'.txt'
    gpsData = []
    
    with open(busIDFileName, 'rb') as busIDFile:
            for line in busIDFile:
                tmpList = line.split(',')
                gpsData.append([int(tmpList[0]),int(tmpList[1]),
                                    float(tmpList[2]),float(tmpList[3])])
    
    return gpsData

def GetFLGPS(gpsData):
    #get all first and last gps fix of a car on each day
    if len(gpsData)==0:
        return []
    
    dayList = []
    gpsFix = []
    dayList.append(gpsData[0][0])
    gpsFix.append([gpsData[0][2],gpsData[0][3],0])
    
    for item in gpsData:
        if not(item[0] in dayList):
            dayList.append(item[0])
            gpsFix.append([lastItem[2],lastItem[3],1])
            gpsFix.append([item[2],item[3],0])            

        lastItem = item
    gpsFix.append([gpsData[-1][2],gpsData[-1][3],1])
    return gpsFix

def GetFirstAFCGPS(busid):
    allGPS = GetProjectedGPS(busid)
    if len(allGPS)==0:
        return []
    busTime = [x[0]*1000000+x[1] for x in allGPS]

    
    
    p = rtree.index.Property()
    p.dimension = 2
    idx = rtree.index.Index(properties=p)
    
    for i in range(len(busTime)):
        idx.add(i, (busTime[i],0))

    busAfcData = ReadAFCByBus(busid)
    if busAfcData == []:
        return []
    
    dayList = []
    gpsFix = []
    dayList.append(busAfcData[0][2])
    afcTime = busAfcData[0][2]*1000000 + busAfcData[0][3]
    busTimeID = list(idx.nearest((afcTime,0)))[0]
    gpsFix.append([allGPS[busTimeID][2],allGPS[busTimeID][3]])

    for item in busAfcData:
        if not(item[2] in dayList):
            dayList.append(item[2])
            afcTime = item[2]*1000000 + item[3]
            #if not (item[3]>7*3600 and item[3]<9*3600):
            #    continue
            #print afcTime
            busTimeID = list(idx.nearest((afcTime,0)))[0]
            gpsFix.append([allGPS[busTimeID][2],allGPS[busTimeID][3]])
    
    return gpsFix

    


def GetRouteOD(routeID):
    #get all first and last gpsfix all cars in routeID
    routeDict = GetRouteIDs(busFileName)
    allODGPS = []
    
    for busid in routeDict[routeID]:
        #gpsData = GetProjectedGPS(busid)
        #gpsFix = GetFLGPS(gpsData)
        gpsFix = GetFirstAFCGPS(busid)
        allODGPS.extend(gpsFix)
        
    return allODGPS


def GetNearbyPoint(point,pList):
    nearest = 100000
    point0 = Point(point[0],point[1])
    for item in pList:
        
        pointX = Point(item[0],item[1])
        dis = Magnitude(point0,pointX)

        if dis < nearest:
            nearest = dis
            nearPoint = item
            
    return nearPoint


def OutputRouteOD(routeID):
    #output 
    allODGPS = GetRouteOD(routeID)
    if not os.path.exists('./route/'):
        os.makedirs('./route/')
    routeODFile = './route/route_od_'+str(routeID)+'.txt'
    OutputLists(allODGPS,routeODFile)

    X = np.array([[item[0],item[1]] for item in allODGPS])
    kmeans = KMeans(init='k-means++', n_clusters=2, n_init=10)
    kmeans.fit(X)
    ODCenter = kmeans.cluster_centers_.tolist()

    OPoint = GetNearbyPoint(ODCenter[0],X.tolist())
    DPoint = GetNearbyPoint(ODCenter[1],X.tolist())

    ODCenter = [OPoint,DPoint]
    
    routeODCenterFile = './route/route_od_center'+str(routeID)+'.txt'
    OutputLists(ODCenter,routeODCenterFile)
    
    return True

def RemoveNoise(oldData,labels):
    newData = []
    for i in range(len(labels)):
        if labels[i]!=-1:
            newData.append(oldData[i])
    return newData

def FarestPoints(points):
    largestDistant = -1
    oPoint = -1
    dPoint = -1
    for i in range(0,len(points)-1):
        for j in range(i+1,len(points)):
            p1 = Point(points[i][0],points[i][1])
            p2 = Point(points[j][0],points[j][1])
            distant = Magnitude(p1,p2)
            if distant>largestDistant:
                largestDistant = distant
                oPoint = i
                dPoint = j
    return [points[oPoint],points[dPoint]]

def OutputRouteOD_Improved(routeID):
    routeODCenterFile = './route/route_od_center'+str(routeID)+'.txt'
    if os.path.isfile(routeODCenterFile):
        return True
    
    allODGPS = GetRouteOD(routeID)
    if not os.path.exists('./route/'):
        os.makedirs('./route/')
    routeODFile = './route/route_od_'+str(routeID)+'.txt'
    OutputLists(allODGPS,routeODFile)

##    X = np.array([[item[0],item[1]] for item in allODGPS])
##    db = DBSCAN(eps=50, min_samples=5).fit(X)
##    labels = db.labels_
##    
##    allODGPS = RemoveNoise(allODGPS,labels)
    
    
    X = np.array([[item[0],item[1]] for item in allODGPS])
    
    if len(allODGPS)>20:
        nCluster=20
    else:
        nCluster=len(allODGPS)
    
    kmeans = KMeans(init='k-means++', n_clusters=nCluster, n_init=10)
    kmeans.fit(X)

    points = kmeans.cluster_centers_.tolist()
    ODCenter = FarestPoints(points)
    
    OPoint = GetNearbyPoint(ODCenter[0],X.tolist())
    DPoint = GetNearbyPoint(ODCenter[1],X.tolist())

    ODCenter = [OPoint,DPoint]
    
    routeODCenterFile = './route/route_od_center'+str(routeID)+'.txt'
    OutputLists(ODCenter,routeODCenterFile)
    
    return True

def FixRouteOD(routeID):
    routeODFile = './route/route_od_'+str(routeID)+'.txt'
    allODs = GetRouteODs(routeID) #remove those outside of boundbox
    
    X = np.array([[item[0],item[1]] for item in allODs])
    
    if len(allODs)>20:
        nCluster=20
    else:
        nCluster=len(allODs)
    
    kmeans = KMeans(init='k-means++', n_clusters=nCluster, n_init=10)
    kmeans.fit(X)

    points = kmeans.cluster_centers_.tolist()
    ODCenter = FarestPoints(points)
    
    OPoint = GetNearbyPoint(ODCenter[0],X.tolist())
    DPoint = GetNearbyPoint(ODCenter[1],X.tolist())

    ODCenter = [OPoint,DPoint]
    
    routeODCenterFile = './route/route_od_center'+str(routeID)+'.txt'
    OutputLists(ODCenter,routeODCenterFile)
    
    return True


def TestDBSCAN(routeID):
    #output 
    allODGPS = GetRouteOD(routeID)
    if not os.path.exists('./route_DBSCAN/'):
        os.makedirs('./route_DBSCAN/')
    

    X = np.array([[item[0],item[1]] for item in allODGPS])
    db = DBSCAN(eps=50, min_samples=5).fit(X)
    labels = db.labels_
    
    
    
    routeODFile = './route_DBSCAN/route_od_'+str(routeID)+'.txt'
    with open(routeODFile, 'w') as f:
        for i in range(len(allODGPS)):
            item = allODGPS[i]
            f.write(str(labels[i])+','+','.join([str(x)for x in item])+'\n')
    
    
        
##    kmeans = KMeans(init='k-means++', n_clusters=2, n_init=10)
##    kmeans.fit(X)
##    
##    ODCenter = kmeans.cluster_centers_.tolist()
##        
##    
##    
##
##    OPoint = GetNearbyPoint(ODCenter[0],X.tolist())
##    DPoint = GetNearbyPoint(ODCenter[1],X.tolist())
##
##    ODCenter = [OPoint,DPoint]
##    
##    routeODCenterFile = './route/route_od_center'+str(routeID)+'.txt'
##    OutputLists(ODCenter,routeODCenterFile)
    
    return True

def GetALLRoutesOD(busFileName):
    routeDict = GetRouteIDs(busFileName)
    for routeID in routeDict.keys():
        #OutputRouteOD(routeID)
        print routeID
        OutputRouteOD_Improved(routeID)
    return True


def FixAllRoutesOD(busFileName):
    routeDict = GetRouteIDs(busFileName)
    for routeID in routeDict.keys():
        #OutputRouteOD(routeID)
        print routeID
        FixRouteOD(routeID)
    return True


def GetRouteODCenters(routeID):
    routeODCenterFile = './route/route_od_center'+str(routeID)+'.txt'
    ods = []
    with open(routeODCenterFile, 'rb') as f:
        for line in f:
            tmpList = line.split(',')
            ods.append([float(tmpList[0]),float(tmpList[1])])
    return ods

def GetRouteODs(routeID):
    routeODFile = './route/route_od_'+str(routeID)+'.txt'
    ods = []
    boundbox = ReadBoundingBox()
    xmin = boundbox[0][0]
    ymin = boundbox[0][1]
    xmax = boundbox[1][0]
    ymax = boundbox[1][1]
    with open(routeODFile, 'rb') as f:
        for line in f:
            tmpList = line.split(',')
            item = [float(tmpList[0]),float(tmpList[1])]
            if item[0]<xmin or item[0]>xmax or item[1]<ymin or item[1]>ymax:
                continue
            ods.append(item)
    return ods

def GetBusGPS(allGPS,day):
    selGPS = []
    for item in allGPS:
        if item[0] == day:
            selGPS.append(item)
    return selGPS

def GetAllBusTrip(busFileName):
    routeDict = GetRouteIDs(busFileName)
    for routeID in routeDict.keys():
        for busID in routeDict[routeID]:
            print routeID,busID
            DivideTrip(routeID,busID)
    return True

def DivideTrip(routeID,busID):
    busTrip = []
    allGPS = GetProjectedGPS(busID)

    tolerance = 500
    
    for day in range(1,10):
        selGPS = GetBusGPS(allGPS,day)
        
        
        if len(selGPS) < 100:
            continue
        
        ods = GetRouteODCenters(routeID)
        pointO = Point(ods[0][0],ods[0][1])
        pointD = Point(ods[1][0],ods[1][1])
        foundFirst = False
        divList = []
        tripOrder = []
        ith = 0
        firstTrip = 0 #1 O->D; -1 D->O
        thisTrip = 0
        for item in selGPS:
            
            pointI = Point(item[2],item[3])
            if not foundFirst:
                if Magnitude(pointO,pointI)<tolerance:
                    foundFirst = True
                    divList.append(ith)
                    
                    firstTrip = 1
                    thisTrip = 1
                    tripOrder.append(thisTrip)
                    
                if Magnitude(pointD,pointI)<tolerance:
                    foundFirst = True
                    divList.append(ith)
                    firstTrip = -1
                    thisTrip = -1
            else:
                if thisTrip == 1:
                    if Magnitude(pointD,pointI)<tolerance:
                        divList.append(ith)
                        thisTrip = -1
                else:
                    if Magnitude(pointO,pointI)<tolerance:
                        divList.append(ith)
                        thisTrip = 1
            ith = ith + 1

        if divList == []:
            print "No suitable trip in bus " + str(busID)+" in day " + str(day)
            continue
        #fix ith with nearest point
        for i in range(0,len(divList)):
            thisTrip = firstTrip*(-1)**i
            ith = divList[i]
            
            
            nearest = 100000
            for k in range(0,30):
                item = selGPS[ith]
                pointI = Point(item[2],item[3])
                if thisTrip == 1:
                    dis = Magnitude(pointO,pointI)
                else:
                    dis = Magnitude(pointD,pointI)

                if dis < nearest:
                    nearest = dis
                    fixith = ith
                    
                ith = ith+1
                if ith>=len(selGPS):
                    break
            divList[i] = fixith
        
        
        #fix first and last point
        firstPoint = Point(selGPS[0][2],selGPS[0][3])
        lastPoint = Point(selGPS[-1][2],selGPS[-1][3])
        firstDivPoint = Point(selGPS[divList[0]][2],selGPS[divList[0]][3])
        lastDivPoint = Point(selGPS[divList[-1]][2],selGPS[divList[-1]][3])
        
        if Magnitude(firstPoint,firstDivPoint)<tolerance:
            divList[0] = 0
        if Magnitude(lastPoint,lastDivPoint)<tolerance:
            divList[-1] = len(selGPS)-1
        
        if divList[0] != 0:
            divList.insert(0,0)
            firstTrip = firstTrip*(-1)
        if divList[-1] != len(selGPS)-1:
            divList.append(len(selGPS)-1)

        tmpList = [day,firstTrip]
        tmpList.extend(divList)
        busTrip.append(tmpList)
##        print day
##        print len(selGPS)
##        print tmpList
    if not os.path.exists('./bus_trip/'):
        os.makedirs('./bus_trip/')
    
    busTripFileName = './bus_trip/bustrip_id'+str(busID)+'.txt'
    OutputLists(busTrip,busTripFileName)

    return True
#--------------------------------------------------------------

#-------------------construct topology-------------------------
def ReadNodes(nodeFileName):
    nodes = {}

    with open(nodeFileName, 'rb') as f:
        for line in f:
            tmpList = line.split(',')
            nodes[int(tmpList[0])] = [float(tmpList[1]),float(tmpList[2])]
    
    return nodes

def ReadEdges(edgeFileName):
    edges = {}

    with open(edgeFileName, 'rb') as f:
        for line in f:
            tmpList = line.split(',')
            if tmpList != []:
                edges[int(tmpList[0])] = [int(tmpList[1]),int(tmpList[2])]

    return edges

def GetBound(point1,point2):
    if point1[0]<point2[0]:
        xmin = point1[0]
        xmax = point2[0]
    else:
        xmin = point2[0]
        xmax = point1[0]
    if point1[1]<point2[1]:
        ymin = point1[1]
        ymax = point2[1]
    else:
        ymin = point2[1]
        ymax = point1[1]
    
    return (xmin,ymin,xmax,ymax)

def CalcEdgeIndex(nodes,edges):
    edgeIdx = rtree.index.Index()
    for edgeid in edges.keys():
        p0 = edges[edgeid][0]
        p1 = edges[edgeid][1]

        bound = GetBound(nodes[p0],nodes[p1])
        edgeIdx.insert(edgeid,bound)
    return edgeIdx
        
def GetEdgeWeight(nodes,edges):
    weight = {}
    for edgeid in edges.keys():
        p0 = edges[edgeid][0]
        p1 = edges[edgeid][1]
        point0 = Point(nodes[p0][0],nodes[p0][1])
        point1 = Point(nodes[p1][0],nodes[p1][1])
        weight[edgeid] = Magnitude(point0,point1)
        
        
    return weight

def GetEdgeAngle(nodes,edges):
    angles = {}
    for edgeid in edges.keys():
        p0 = edges[edgeid][0]
        p1 = edges[edgeid][1]
        point0 = Point(nodes[p0][0],nodes[p0][1])
        point1 = Point(nodes[p1][0],nodes[p1][1])
        angles[edgeid] = GetAngle(point0,point1)
        
    return angles


def ConstructRoadNetwork(nodeFileName,edgeFileName):
    print 'Constructing Road Network'
    nodes = ReadNodes(nodeFileName)
    edges = ReadEdges(edgeFileName)
    edgeIdx = CalcEdgeIndex(nodes,edges)
    edgeWeight = GetEdgeWeight(nodes,edges)
    edgeAngle = GetEdgeAngle(nodes,edges)

    rdNet = nx.DiGraph()
    for edgeid in edges.keys():
        sNode = edges[edgeid][0] #start node
        eNode = edges[edgeid][1] #end node
        rdNet.add_edge(sNode,eNode,weight=edgeWeight[edgeid])
        rdNet[sNode][eNode]['ID'] = edgeid
        rdNet[sNode][eNode]['angle'] = edgeAngle[edgeid]
    return nodes,edges,edgeIdx,rdNet
    

#--------------------------------------------------------------

#-------------------route inference----------------------------

def BatchGPSAngle(gpsfix):
    angles = []
    p1 = Point(gpsfix[0][0],gpsfix[0][1])
    p2 = Point(gpsfix[1][0],gpsfix[1][1])
    lastAngle = GetAngle(p1,p2)
    angles.append(lastAngle)
    
    for i in range(1,len(gpsfix)-1):
        p1 = Point(gpsfix[i][0],gpsfix[i][1])
        p2 = Point(gpsfix[i+1][0],gpsfix[i+1][1])
        
        if Magnitude(p1,p2)<1e-5:
            thisAngle = lastAngle
        else:
            thisAngle = GetAngle(p1,p2)
            
        angles.append((thisAngle+lastAngle)/2)
        lastAngle = thisAngle

    angles.append(thisAngle)
    
    return angles

def MapMatching(tripGPS,nodes,edges,edgeIdx,rdNet):
    #tripGPS: tripTimes,tripXs,tripYs,tripAngle
    #initialization
    nodeSequence = []
    edgeSequence = []
    ind = -1
    num_points = len(tripGPS)
    gpsError = 160.0
    weightD = 1.0
    weightH = 0.2
    weightS = 1.0
    
    #skip points of the same position
    while True:
        ind = ind+1
        if ind>=num_points-1:
            break
        p1 = Point(tripGPS[ind][1],tripGPS[ind][2])
        p2 = Point(tripGPS[ind+1][1],tripGPS[ind+1][2])
        if Magnitude(p1,p2)>1e-5:
            break
        

    
    foundEdge= False
    while not foundEdge:
        ind=ind+1
        if ind>=num_points:
            break;
        
        p1 = Point(tripGPS[ind][1],tripGPS[ind][2])
        bound = (p1.x-gpsError,p1.y-gpsError,p1.x+gpsError,p1.y+gpsError)
        
        candidateEdges = list(edgeIdx.intersection(bound))
        
        bestScore = -10000
        
        for edgeid in candidateEdges:
            nodeStartId = edges[edgeid][0]
            nodeEndId = edges[edgeid][1]
            startPoint = Point(nodes[nodeStartId][0],
                               nodes[nodeStartId][1])
            endPoint = Point(nodes[nodeEndId][0],
                             nodes[nodeEndId][1])
            disToNextLink = Magnitude(p1,endPoint)
            distance,notFallInLine = DisToLine(p1,startPoint,endPoint)
            if distance>gpsError or notFallInLine:
                continue
            angleDiff = tripGPS[ind][3]- \
                        rdNet[nodeStartId][nodeEndId]['angle']
            
            wP = weightD*(80-distance)/80
            wH = weightH*math.cos(angleDiff/180.0*math.pi)

            TWS = wP+wH

            if TWS>bestScore:
                bestScore = TWS
                bestEdge = edgeid
                foundEdge = True
        if foundEdge:
            nodeSequence.append(nodeStartId)
            nodeSequence.append(nodeEndId)
            edgeSequence.append(edgeid)


    lastP = p1
    lastDisToNextLink = disToNextLink
    
    while True:
        
        ind=ind+1
        print ind
        if ind>=num_points:
            break;
        

        p1 = Point(tripGPS[ind][1],tripGPS[ind][2])
        timeDiff = tripGPS[ind][0]-tripGPS[ind-1][0]
        shortDis = Magnitude(p1,lastP)
        if shortDis<1e-5:
            continue
        
        bound = (p1.x-gpsError,p1.y-gpsError,p1.x+gpsError,p1.y+gpsError)
        
        candidateEdges = list(edgeIdx.intersection(bound))
        
        bestScore = -10000
        foundEdge = False
        for edgeid in candidateEdges:
            nodeStartId = edges[edgeid][0]
            nodeEndId = edges[edgeid][1]
            startPoint = Point(nodes[nodeStartId][0],
                               nodes[nodeStartId][1])
            endPoint = Point(nodes[nodeEndId][0],
                             nodes[nodeEndId][1])
            disToNextLink = Magnitude(p1,endPoint)
            
            distance,notFallInLine = DisToLine(p1,startPoint,endPoint)
            if distance>gpsError or notFallInLine:
                continue
            angleDiff = tripGPS[ind][3]- \
                        rdNet[nodeStartId][nodeEndId]['angle']
            
            wP = weightD*(80-distance)/80.0
            wH = weightH*math.cos(angleDiff/180.0*math.pi)

            TWS = wP+wH
##            if edgeid==edgeSequence[-1]:
##                speed = shortDis/timeDiff
##                shortPath = []
##            else:
##                shortPath = nx.shortest_path(rdNet,nodeSequence[-1],nodeEndId,'weight')
##                sumDistance = 0
##                for i in range(len(shortPath)-1):
##                    sumDistance = sumDistance+rdNet[shortPath[i]][shortPath[i+1]]['weight']
##                sumDistance = sumDistance+lastDisToNextLink-disToNextLink
##                speed = sumDistance/timeDiff
##
##            if speed>30:
##                continue
##            wS = weightS*(30-speed)/30
            
##            TWS = wP+wH+wS
            
            
            if TWS>bestScore:
                bestScore = TWS
                bestEdge = edgeid
                bestToNextLink = disToNextLink
                foundEdge = True
                

        if foundEdge:
            try:
                if bestEdge==edgeSequence[-1]:
                    shortPath = []
                else:
                    nodeEndId = edges[bestEdge][1]
                    shortPath = nx.shortest_path(rdNet,nodeSequence[-1], nodeEndId,'weight')
                    sumDistance = 0
                    for i in range(len(shortPath)-1):
                        sumDistance = sumDistance+rdNet[shortPath[i]][shortPath[i+1]]['weight']
                    sumDistance = sumDistance+lastDisToNextLink-bestToNextLink
                    speed = sumDistance/timeDiff
                    if speed>30:
                        continue
                    
            except:
                print 'error in shorest_path'
                continue

            
            lastP = p1
            lastDisToNextLink = bestToNextLink
            for i in range(len(shortPath)-1):
                nodeSequence.append(shortPath[i+1])
                edgeSequence.append(rdNet[shortPath[i]][shortPath[i+1]]['ID'])
        
        
    return nodeSequence


def AggregateNetwork(tripGPS,nodes,edges,edgeIdx,rdNet):
    gpsError = 100.0
    weightD = 1.0
    weightH = 0.2
    weightS = 1.0

    
    for ind in range(len(tripGPS)):
        #print ind
        p1 = Point(tripGPS[ind][1],tripGPS[ind][2])
        
        bound = (p1.x-gpsError,p1.y-gpsError,p1.x+gpsError,p1.y+gpsError)
        
        candidateEdges = list(edgeIdx.intersection(bound))
        for edgeid in candidateEdges:
            nodeStartId = edges[edgeid][0]
            nodeEndId = edges[edgeid][1]
            startPoint = Point(nodes[nodeStartId][0],
                               nodes[nodeStartId][1])
            endPoint = Point(nodes[nodeEndId][0],
                             nodes[nodeEndId][1])
            disToNextLink = Magnitude(p1,endPoint)
            
            distance,notFallInLine = DisToLine(p1,startPoint,endPoint)
            if distance>gpsError:
                continue
            angleDiff = tripGPS[ind][3]- \
                        rdNet[nodeStartId][nodeEndId]['angle']
            
            wP = weightD*(50-distance)/50.0
            wH = weightH*math.cos(angleDiff/180.0*math.pi)

            TWS = wP+wH
            rdNet[nodeStartId][nodeEndId]['weight'] = rdNet[nodeStartId][nodeEndId]['weight']/(TWS+weightH+weightD+1)

    
    return rdNet

def GetNearbyPointID(p,nodes,edges,edgeIdx):
    error = 800
    bound = (p.x-error,p.y-error,p.x+error,p.y+error)
    candidateEdges = list(edgeIdx.intersection(bound))

    nearest = 800
    for edgeid in candidateEdges:
        nodeStartId = edges[edgeid][0]
        nodeEndId = edges[edgeid][1]
        startPoint = Point(nodes[nodeStartId][0],
                           nodes[nodeStartId][1])
        endPoint = Point(nodes[nodeEndId][0],
                         nodes[nodeEndId][1])
        disToNextLink = Magnitude(p,endPoint)
        
        distance,notFallInLine = DisToLine(p,startPoint,endPoint)

        if distance<nearest:
            nearest = distance
            foundPoint = nodeStartId
            

    return foundPoint

def GetNearbyPointIDs(p,nodes,edges,edgeIdx,num_points=10):
    error = 800
    bound = (p.x,p.y,p.x,p.y)
    candidateEdges = list(edgeIdx.nearest(bound,num_points))

    nearest = 800
    foundPoints = []
    for edgeid in candidateEdges:
        nodeStartId = edges[edgeid][0]
        foundPoints.append(nodeStartId)
##        nodeEndId = edges[edgeid][1]
##        startPoint = Point(nodes[nodeStartId][0],
##                           nodes[nodeStartId][1])
##        endPoint = Point(nodes[nodeEndId][0],
##                         nodes[nodeEndId][1])
##        disToNextLink = Magnitude(p,endPoint)
##        
##        distance,notFallInLine = DisToLine(p,startPoint,endPoint)

##        if distance<nearest:
##            nearest = distance
##            foundPoint = nodeStartId
            

    return foundPoints
            
def RouteInference(nodeFileName,edgeFileName,busFileName):
    routeDict = GetRouteIDs(busFileName)
    enoughRoute = 50
    if not os.path.exists('./route_infer/'):
        os.makedirs('./route_infer/')
    nodes,edges,edgeIdx,rdNet = ConstructRoadNetwork(nodeFileName,edgeFileName)
    for routeID in routeDict.keys():
        print routeID
        fileName = './route_infer/route_od_'+str(routeID)+'.txt'
        if os.path.isfile(fileName):
            continue
        
        rdNetO = rdNet.copy()
        rdNetD = rdNet.copy()
        
        ods = GetRouteODCenters(routeID)
        startP = Point(ods[0][0],ods[0][1])
        endP = Point(ods[1][0],ods[1][1])
        startPointId = GetNearbyPointID(startP,nodes,edges,edgeIdx)
        endPointId = GetNearbyPointID(endP,nodes,edges,edgeIdx)
        aggregate_o = 0
        aggregate_d = 0
        for busID in routeDict[routeID]:
            if aggregate_o>enoughRoute and aggregate_d>enoughRoute:
                break
            #print aggregate_num
            dividedTrip = GetBusDividedTrip(busID)
            
            for i in range(1,len(dividedTrip)-1):
                if aggregate_o>enoughRoute and aggregate_d>enoughRoute:
                    break
                tripGPS = dividedTrip[i][1]
                if dividedTrip[i][0] == 1:
                    aggregate_o = aggregate_o + 1
                    if aggregate_o>enoughRoute:
                        continue
                    rdNetO = AggregateNetwork(tripGPS,nodes,edges,edgeIdx,rdNetO)
                else:
                    aggregate_d = aggregate_d + 1
                    if aggregate_d>enoughRoute:
                        continue
                    rdNetD = AggregateNetwork(tripGPS,nodes,edges,edgeIdx,rdNetD)
        noPath = True
        try:
            ODSequence = nx.shortest_path(rdNetO,startPointId, endPointId,'weight')
            DOSequence = nx.shortest_path(rdNetD,endPointId,startPointId, 'weight')
            noPath = False
        except nx.exception.NetworkXNoPath as e:
            print e

        if noPath:
            
            startPointIds = GetNearbyPointIDs(startP,nodes,edges,edgeIdx)
            endPointIds = GetNearbyPointIDs(endP,nodes,edges,edgeIdx)
            for sp in startPointIds:
                if not noPath:
                    break
                for ep in endPointIds:
                    if not noPath:
                        break
                    try:
                        ODSequence = nx.shortest_path(rdNetO,sp, ep,'weight')
                        DOSequence = nx.shortest_path(rdNetD,ep, sp, 'weight')
                        noPath = False
                    except nx.exception.NetworkXNoPath as e:
                        print e
                
        if noPath:
            sys.exit(0)
        fileName = './route_infer/route_od_'+str(routeID)+'.txt'
        OutputNodeSeq(ODSequence,nodes,fileName)
        fileName = './route_infer/route_do_'+str(routeID)+'.txt'
        OutputNodeSeq(DOSequence,nodes,fileName)
        
    return True


def GetBusDividedTrip(busID):
    allGPS = GetProjectedGPS(busID)

    busTrip = []
    busTripFileName = './bus_trip/bustrip_id'+str(busID)+'.txt'
    with open(busTripFileName, 'rb') as f:
        for line in f:
            busTrip.append([int(item) for item in line.split(',')])
    dividedTrip = []
    for item in busTrip:
        day = item[0]
        direction = item[1]
        divList = item[2:len(item)]
        selGPS = GetBusGPS(allGPS,day)
        gpsFix = [[x[2],x[3]] for x in selGPS]
        angles = BatchGPSAngle(gpsFix)
        for i in range(len(divList)-1):
            start = divList[i]
            end = divList[i+1]
            eachTrip = selGPS[start:end]
            tripTimes = [tmp[1] for tmp in eachTrip]
            tripXs = [tmp[2] for tmp in eachTrip]
            tripYs = [tmp[3] for tmp in eachTrip]
            tripAngle = angles[start:end]
            thisTripGPS = zip(tripTimes,tripXs,tripYs,tripAngle)
            #nodeSequence = MapMatching(thisTripGPS,nodes,edges,rdNet)
            direction = direction*(-1)
            dividedTrip.append([direction,thisTripGPS]);
    
    
    return dividedTrip
#--------------------------------------------------------------

#----------------------judge direction --------------------
def CorrectDayTrip(orders):
    
    while True:
        found = False
        foundInd = -1
        for i in range(len(orders)-1):
            start = orders[i]
            end = orders[i+1]
            if end<start:
                found = True
                foundInd = i
                break

        if not found:
            break

        if foundInd<=len(orders)-4:
            del orders[foundInd+1]
            del orders[foundInd+1]
        else:
            break
        
    return orders     
            

        
def RestoreDirection(dayTrip):
    
    day = dayTrip[0]
    direction = dayTrip[1]
    sections = dayTrip[2:]
    directionList = []

    sections = CorrectDayTrip(sections)

    
    for i in range(len(sections)-1):
        
        start = sections[i]
        end = sections[i+1]
        num = end-start
        thisSection = [direction for tmp in range(num)]
        directionList.extend(thisSection)

        direction = direction*(-1)

        
    
    directionList.extend([direction*(-1)])

    return directionList
            
def CalcDirection(gpsFix,pathIdxOD):
    #gpsFix: day time x y
    directionList = []
    direction = 1
    thisP = Point(gpsFix[0][2],gpsFix[0][3])
    thisIdx = list(pathIdxOD.nearest((thisP.x,thisP.y)))[0]
    for i in range(len(gpsFix)-1):
        nextP = Point(gpsFix[i+1][2],gpsFix[i+1][3])
        nextIdx = list(pathIdxOD.nearest((nextP.x,nextP.y)))[0]
        if (nextIdx-thisIdx)>=0:
            direction = 1
        else:
            direction = -1
        directionList.append(direction)
        thisIdx = nextIdx
    
    directionList.append(direction)
    return directionList

def JudgeDirection(busID,pathIdxOD):
    allGPS = GetProjectedGPS(busID)
    allDay = list(set([item[0] for item in allGPS]))
    busTrip = []
    busTripFileName = './bus_trip/bustrip_id'+str(busID)+'.txt'
    with open(busTripFileName, 'rb') as f:
        for line in f:
            busTrip.append([int(item) for item in line.split(',')])
    haveDay = [item[0] for item in busTrip]
    allDirection = []
    for day in allDay:
        selGPS = GetBusGPS(allGPS,day)
        if day in haveDay:
            dayTrip = busTrip[haveDay.index(day)]
            dayDirection = RestoreDirection(dayTrip)
            
            if len(selGPS)!=len(dayDirection):
                dayDirection = CalcDirection(selGPS,pathIdxOD)
        else:
            
            dayDirection = CalcDirection(selGPS,pathIdxOD)
        allDirection.extend(dayDirection)
    if len(allDirection)!=len(allGPS):
        print 'error occurs at '+str(busID)
        sys.exit(0)
    return allDirection
    
def JudgeDirectionAllBus(busFileName):
    if not os.path.exists('./bus_direction/'):
        os.makedirs('./bus_direction/')
    routeDict = GetRouteIDs(busFileName)
    for routeID in routeDict.keys():
        fileName = './route_infer/route_od_'+str(routeID)+'.txt'
        busPathOD = ReadPath(fileName)
        disPathOD = DiscretizePath(busPathOD)
        pathIdxOD = BuildIndexForPath(disPathOD)
        for busID in routeDict[routeID]:
            print busID
            fileName = './bus_direction/bus_direction_'+str(busID)+'.txt'
            if os.path.isfile(fileName):
                continue
            allDirection = JudgeDirection(busID,pathIdxOD)
            OutputSimpleList(allDirection,fileName)

    return True
            
    

#--------------------------------------------------------------

#-------------------Get Bus Stop------------------------------


def GetAFCData(afcFileName):
    afcData = []
    with open(afcFileName, 'rb') as f:
        f.readline()
        for line in f:
            tmpList = [int(x) for x in line.split(',')]
            afcData.append(tmpList)
    return afcData
            

def GetAFCByBus(busID,afcData):
    
    busAfcData = []
    for item in afcData:
        if item[4]==busID:
            busAfcData.append(item)
    index = [x[2]*1000000+x[3] for x in busAfcData]
    
    sortBusAfcData = [x for (y,x) in sorted(zip(index,busAfcData))]

    
    return sortBusAfcData

def ReorganizeAFCByBus(afcFileName,busFileName):
    afcData = GetAFCData(afcFileName)
    routeDict = GetRouteIDs(busFileName)

    if not os.path.exists('./afc_bus/'):
        os.makedirs('./afc_bus/')
        
    for routeID in routeDict.keys():
        for busID in routeDict[routeID]:
            
            fileName = './afc_bus/bus_afc_'+str(busID)+'.txt'
            if os.path.isfile(fileName):
                continue
            
            sortBusAfcData = GetAFCByBus(busID,afcData)
            OutputLists(sortBusAfcData,fileName)

    return True

def ReorganizeAFCByCardID(afcFileName):

    if not os.path.exists('./afc_cardid/'):
        os.makedirs('./afc_cardid/')
    afcData = GetAFCData(afcFileName)
    cardIds  = list(set([tmp[1] for tmp in afcData]))
    afcDict = {}
    for cardId in cardIds:
        afcDict[cardId] = []

    for item in afcData:
        afcDict[item[1]].append(item)

    for cardId in cardIds:
        fileName = './afc_cardid/card_afc_'+str(cardId)+'.txt'
        if os.path.isfile(fileName):
                continue
        cardAfcData = afcDict[cardId]
        
        index = [x[2]*1000000+x[3] for x in cardAfcData]
        sortCardAfcData = [x for (y,x) in sorted(zip(index,cardAfcData))]
        
        OutputLists(sortCardAfcData,fileName)
    
    return True

def ReadAFCByBus(busID):
    fileName = './afc_bus/bus_afc_'+str(busID)+'.txt'
    busAfcData = []
    with open(fileName, 'rb') as f:
        for line in f:
            tmpList = [int(x) for x in line.split(',')]
            busAfcData.append(tmpList)
    return busAfcData



def GetAFCCountByBus(busID, tolerance = 10):
    busAfcData = ReadAFCByBus(busID)
    afcTime = [x[2]*1000000+x[3] for x in busAfcData]
    
    allGPS = GetProjectedGPS(busID)
    if len(allGPS)==0:
        return []
    busTime = [x[0]*1000000+x[1] for x in allGPS]

    p = rtree.index.Property()
    p.dimension = 2
    idx = rtree.index.Index(properties=p)
    
    for i in range(len(busTime)):
        idx.add(i, (busTime[i],0))
        
    count = [0 for x in range(len(busTime))]

    for aTime in afcTime:
        busTimeID = list(idx.nearest((aTime,0)))[0]
        if math.fabs(busTime[busTimeID]-aTime)<tolerance:
            count[busTimeID] = count[busTimeID]+1

    
    return count

def GetAFCCountAllBus(busFileName):
    if not os.path.exists('./bus_count/'):
        os.makedirs('./bus_count/')
    routeDict = GetRouteIDs(busFileName)
    for routeID in routeDict.keys():
        for busID in routeDict[routeID]:
            print busID
            fileName = './bus_count/bus_count_'+str(busID)+'.txt'
            if os.path.isfile(fileName):
                continue
            busCount = GetAFCCountByBus(busID)
            OutputSimpleList(busCount,fileName)

    return True


def ReadPath(fileName):
    busPath = []
    with open(fileName, 'rb') as f:
        for line in f:
            tmpList = line.split(',')
            busPath.append([int(tmpList[0]),float(tmpList[1]),float(tmpList[2])])
    return busPath

def DiscretizePath(busPath,interval=2):
    #input: busPath get from file
    #output: discretize path with certain interval
    #start from 1
    num_p = 0
    disPath = []
    
    num_p = num_p+1
    disPath.append([num_p,busPath[0][1],busPath[0][2]])
    
    pLast = Point(busPath[0][1],busPath[0][2])
    for i in range(1,len(busPath)):
        pCurrent = Point(busPath[i][1],busPath[i][2])
        length = Magnitude(pLast,pCurrent)
        angle = GetAngle(pLast,pCurrent)
        xStep = interval*math.cos(angle/180*math.pi)
        yStep = interval*math.sin(angle/180*math.pi)
        for i in range(1,int(math.floor(length/interval))):
            num_p = num_p+1
            disPath.append([num_p,pLast.x+xStep*i,pLast.y+yStep*i])
        num_p = num_p+1
        disPath.append([num_p,pCurrent.x,pCurrent.y])

        pLast = pCurrent
        
    return disPath

def BuildIndexForPath(disPath):
    p = rtree.index.Property()
    p.dimension = 2
    idx = rtree.index.Index(properties=p)
    
    for p in disPath:
        idx.add(p[0], (p[1],p[2]))
    
    return idx

def ReadBusCount(busID):
    fileName = './bus_count/bus_count_'+str(busID)+'.txt'
    
    f = open(fileName, 'r')
    lines = f.readlines()
    f.close()

    count = [int(x) for x in lines]
    return count

def ReadBusDirection(busID):
    fileName = './bus_direction/bus_direction_'+str(busID)+'.txt'
    
    f = open(fileName, 'r')
    lines = f.readlines()
    f.close()

    direction = [int(x) for x in lines]
    return direction

def AggregateBusStop(busFileName):
    routeDict = GetRouteIDs(busFileName)
    enoughRoute = 50
    aggregate_o = 0
    
    for routeID in routeDict.keys():
        print routeID
        fileName = './route_infer/route_od_dis'+str(routeID)+'.txt'
        if os.path.isfile(fileName):
            continue
        
        fileName = './route_infer/route_od_'+str(routeID)+'.txt'
        busPathOD = ReadPath(fileName)
        disPathOD = DiscretizePath(busPathOD)
        pathIdxOD = BuildIndexForPath(disPathOD)
        afcCountOD = [0 for x in disPathOD]

        fileName = './route_infer/route_do_'+str(routeID)+'.txt'
        busPathDO = ReadPath(fileName)
        disPathDO = DiscretizePath(busPathDO)
        pathIdxDO = BuildIndexForPath(disPathDO)
        afcCountDO = [0 for x in disPathDO]
        
        for busID in routeDict[routeID]:
            busCount = ReadBusCount(busID)
            busDirection = ReadBusDirection(busID)
            allGPS = GetProjectedGPS(busID)
            
            for i in range(len(busCount)):
                if busCount[i]==0:
                    continue
                if busDirection[i]==1: #od forward direction
                    foundIdx = list(pathIdxOD.nearest((allGPS[i][2],allGPS[i][3])))[0]
                    afcCountOD[foundIdx-1] = afcCountOD[foundIdx-1]+busCount[i]
                else:
                    foundIdx = list(pathIdxDO.nearest((allGPS[i][2],allGPS[i][3])))[0]
                    afcCountDO[foundIdx-1] = afcCountDO[foundIdx-1]+busCount[i]

        fileName = './route_infer/route_od_dis'+str(routeID)+'.txt'
        OutputDisPathCount(disPathOD,afcCountOD,fileName)
        fileName = './route_infer/route_do_dis'+str(routeID)+'.txt'
        OutputDisPathCount(disPathDO,afcCountDO,fileName)

    return True


def ReadPathAfc(fileName):
    disPathAfc = []
    with open(fileName, 'rb') as f:
        for line in f:
            tmpList = line.split(',')
            disPathAfc.append([int(tmpList[0]),float(tmpList[1]),float(tmpList[2]),int(tmpList[3])])
    return disPathAfc

def FilterPathAfc(disPathAfc,interval=50,low_bound=3):
    ids = [tmp[0] for tmp in disPathAfc]
    count = [tmp[3] for tmp in disPathAfc]

    for i in range(len(count)):
        if count[i]<low_bound:
            count[i]=0
    
    
    changed = True

    iterTimes = 0
    sections = len(count)/interval
    while changed:
        iterTimes = iterTimes+1
        
        print iterTimes
        chaged = False
        devi = iterTimes/interval
        new_count = [0 for tmp in count]
        for i in range(sections):
            startInd = i*interval+devi
            endInd = (i+1)*interval+devi
            if endInd >len(count):
                endInd = len(count)
            countSum = sum(count[startInd:endInd])
            if countSum==0:
                continue
            centerInd = sum([a*b for a,b in zip(count[startInd:endInd],ids[startInd:endInd])])/countSum
            
            new_count[centerInd-1] = countSum
        if new_count != count:
            chaged = True
            count = new_count
    
    return count


def GetBusStop(ids,count,labels):
    sumCount = [0 for tmp in set(labels)]
    sumCountID = [0 for tmp in set(labels)]
    for i in range(len(ids)):
        label = labels[i]
        if label==-1:
            continue
        if count[i]==0:
            continue
        
        sumCount[label]=sumCount[label]+count[i]
        sumCountID[label]=sumCountID[label]+count[i]*ids[i]

    busStops = []
    for label in set(labels):
        if label==-1:
            continue 
        centerID = int(round(sumCountID[label]/float(sumCount[label])))
        
        busStops.append([centerID,sumCount[label]])
    return busStops




def BusStopDBScan(disPathAfc):
    
    ids = [tmp[0] for tmp in disPathAfc]
    xs = [tmp[1] for tmp in disPathAfc]
    ys = [tmp[2] for tmp in disPathAfc]
    count = [tmp[3] for tmp in disPathAfc]
    
    X = np.array([[tmp] for tmp in ids])
    #can be modified
    db = DBSCAN(eps=3, min_samples=13).fit(X,sample_weight = count)
    labels = db.labels_
    
    busStops = GetBusStop(ids,count,labels)

    busStopsWithCoords = []
    for item in busStops:
        thisID = item[0]
        thisCount = item[1]
        thisX = xs[thisID-1]
        thisY = ys[thisID-1]
        busStopsWithCoords.append([thisID,thisCount,thisX,thisY])
    return busStopsWithCoords

def BusStopKMeans(disPathAfc):
    ids = [tmp[0] for tmp in disPathAfc]
    xs = [tmp[1] for tmp in disPathAfc]
    ys = [tmp[2] for tmp in disPathAfc]
    count = [tmp[3] for tmp in disPathAfc]

    tmpArray = []
    hasAfc = 0
    for i in range(0,len(ids)):
        if count[i]>0:
            hasAfc = hasAfc+1
        for j in range(0,count[i]):
            tmpArray.append([ids[i]])
    
    X = np.array(tmpArray)

    if hasAfc> 500:
        nClusters = 500
    else:
        nClusters = hasAfc
    kmeans = MiniBatchKMeans(n_clusters=nClusters, n_init=10, init_size=1000)
    kmeans.fit(X)
    busStops = kmeans.cluster_centers_.tolist()
    #print busStops
    busStopsWithCoords = []

    busStops.sort()
    for item in busStops:
        thisID = int(round(item[0]))
        thisCount = 0
        thisX = xs[thisID-1]
        thisY = ys[thisID-1]
        busStopsWithCoords.append([thisID,thisCount,thisX,thisY])
    return busStopsWithCoords


def OutputBusStop(routeID):    
    if not os.path.exists('./route_stop/'):
        os.makedirs('./route_stop/')
    
    fileName = './route_infer/route_od_dis'+str(routeID)+'.txt'
    disPathAfcOD = ReadPathAfc(fileName)
    #countOD = FilterPathAfc(disPathAfcOD)
    #busStopsWithCoordsOD = BusStopDBScan(disPathAfcOD)
    busStopsWithCoordsOD = BusStopKMeans(disPathAfcOD)
    
    
    fileName = './route_infer/route_do_dis'+str(routeID)+'.txt'
    disPathAfcDO = ReadPathAfc(fileName)
    #countDO = FilterPathAfc(disPathAfcDO)
    #busStopsWithCoordsDO = BusStopDBScan(disPathAfcDO)
    busStopsWithCoordsDO = BusStopKMeans(disPathAfcDO)



    busStopsWithCoordsOD = AddOppositeBusStop(disPathAfcOD,busStopsWithCoordsOD,busStopsWithCoordsDO)
    busStopsWithCoordsDO = AddOppositeBusStop(disPathAfcDO,busStopsWithCoordsDO,busStopsWithCoordsOD)

    fileName = './route_stop/route_stop_od'+str(routeID)+'.txt'
    OutputLists(busStopsWithCoordsOD,fileName)
    fileName = './route_stop/route_stop_do'+str(routeID)+'.txt'
    OutputLists(busStopsWithCoordsDO,fileName)

def AddOppositeBusStop(disPathAfc,busStop,busStopOpp):
    disTolerance = 50

    xs = [tmp[1] for tmp in disPathAfc]
    ys = [tmp[2] for tmp in disPathAfc]
    
    p = rtree.index.Property()
    p.dimension = 2
    idx = rtree.index.Index(properties=p)
    
    for item in disPathAfc:
        idx.add(item[0], (item[1],item[2]))

    busIds = [item[0] for item in busStop]
    newIds = []
    for item in busStopOpp:
        px = item[2]
        py = item[3]
        nearID = list(idx.nearest((px,py)))[0]
        leftInd = bisect_left(busIds,nearID)-1
        rightInd = leftInd+1
        if leftInd <0:
            leftInd=0
        
        if rightInd==len(busIds):
            rightInd = leftInd
        disLeft = abs(nearID-busIds[leftInd])
        disRight = abs(nearID-busIds[rightInd])
        if disLeft>50 and disRight>50:
            newIds.append(nearID)
        
    busIds.extend(newIds)
    busIds.sort()

    busStopsWithCoords = []
    for item in busIds:
        thisID = item
        if item in newIds:
            thisCount = 0
        else:
            thisCount = 1
        thisX = xs[thisID-1]
        thisY = ys[thisID-1]
        busStopsWithCoords.append([thisID,thisCount,thisX,thisY])
        
    return busStopsWithCoords
    

def OutputAllRoute(busFileName):
    routeDict = GetRouteIDs(busFileName)
    for routeID in routeDict.keys():
        print routeID
        OutputBusStop(routeID)
    return True

def IntervalSample(routeStops,disPathAfc):
    minDistance = 400
    minPoints = minDistance/2

    xs = [tmp[1] for tmp in disPathAfc]
    ys = [tmp[2] for tmp in disPathAfc]
    
    
    
    ids = [tmp[0] for tmp in routeStops]
    newId = []
    for i in range(0,len(ids)-1):
        newId.append([ids[i],1])
        numPoints = int(round(float(ids[i+1]-ids[i])/minPoints))
        
        if numPoints==0:
            continue
        intervalBetween =  (ids[i+1]-ids[i])/(numPoints+1)
        
        for j in range(0,numPoints):
            tmpID = ids[i]+intervalBetween*(j+1)
            newId.append([tmpID,0])
    newId.append([ids[-1],1])


    busStopsWithCoords = []
    for item in newId:
        thisID = item[0]
        
        thisCount = item[1]
        thisX = xs[thisID-1]
        thisY = ys[thisID-1]
        busStopsWithCoords.append([thisID,thisCount,thisX,thisY])
    return busStopsWithCoords

def OutputIntervalBusStop(routeID):    
    if not os.path.exists('./route_stop/'):
        os.makedirs('./route_stop/')
    
    fileName = './route_infer/route_od_dis'+str(routeID)+'.txt'
    disPathAfcOD = ReadPathAfc(fileName)
    fileName = './route_stop/route_stop_od'+str(routeID)+'.txt'
    routeStopsOD = ReadRouteStopID(fileName)
    busStopsWithCoordsOD = IntervalSample(routeStopsOD,disPathAfcOD)
    fileName = './route_stop/route_stop_interval_od'+str(routeID)+'.txt'
    OutputLists(busStopsWithCoordsOD,fileName)
    
    fileName = './route_infer/route_do_dis'+str(routeID)+'.txt'
    disPathAfcDO = ReadPathAfc(fileName)
    fileName = './route_stop/route_stop_do'+str(routeID)+'.txt'
    routeStopsDO = ReadRouteStopID(fileName)
    busStopsWithCoordsDO = IntervalSample(routeStopsDO,disPathAfcDO)
    fileName = './route_stop/route_stop_interval_do'+str(routeID)+'.txt'
    OutputLists(busStopsWithCoordsDO,fileName)    

def OutputAllIntervalRoute(busFileName):
    routeDict = GetRouteIDs(busFileName)
    for routeID in routeDict.keys():
        print routeID
        OutputIntervalBusStop(routeID)
    return True

def ReadRouteStopID(fileName):
    routeStopsID =[]
    sequence = 0
    with open(fileName,'r') as f:
        for line in f:
            sequence = sequence+1
            tmpList = line.split(',')
            
            routeStopsID.append([int(tmpList[0])])
    return routeStopsID

def ReadRouteStop(fileName,project=True):
    routeStops =[]
    sequence = 0
    with open(fileName,'r') as f:
        for line in f:
            sequence = sequence+1
            tmpList = line.split(',')
            
            routeStops.append([float(tmpList[2]),float(tmpList[3])])
    if project:
        return ProjectCoors(routeStops,True)
    else:
        return routeStops


def SubmitIntervalBusStop(busFileName):
##    (stop_id, route_id, direction, sequence, lng, lat)
##    1,R1, 0, 1, 112.032, 30.042
    if not os.path.exists('./results/'):
        os.makedirs('./results/')
    resultFileName = './results/RESULT_STOP_LIST.csv'
    routeDict = GetRouteIDs(busFileName)
    stopID = 1
    allRecords = []
    for routeID in routeDict.keys():
        fileName = './route_stop/route_stop_interval_od'+str(routeID)+'.txt'
        routeStopsOD = ReadRouteStop(fileName)
        num_stops = len(routeStopsOD)
        stopIds = range(stopID,num_stops+stopID)
        stopID = num_stops+stopID
        routeIds = [routeID for tmp in range(num_stops)]
        directions = [0 for tmp in range(num_stops)]
        sequences = range(1,1+num_stops)
        lngs = [item[0] for item in routeStopsOD]
        lats = [item[1] for item in routeStopsOD]
        allRecords.extend(zip(stopIds,routeIds,directions,sequences,lngs,lats))

        
        fileName = './route_stop/route_stop_interval_do'+str(routeID)+'.txt'
        routeStopsDO = ReadRouteStop(fileName)
        num_stops = len(routeStopsDO)
        stopIds = range(stopID,num_stops+stopID)
        stopID = num_stops+stopID
        routeIds = [routeID for tmp in range(num_stops)]
        directions = [1 for tmp in range(num_stops)]
        sequences = range(1,1+num_stops)
        lngs = [item[0] for item in routeStopsDO]
        lats = [item[1] for item in routeStopsDO]
        allRecords.extend(zip(stopIds,routeIds,directions,sequences,lngs,lats))

    OutputLists(allRecords,resultFileName)
    
    return allRecords

def SubmitBusStop(busFileName):
##    (stop_id, route_id, direction, sequence, lng, lat)
##    1,R1, 0, 1, 112.032, 30.042
    if not os.path.exists('./results/'):
        os.makedirs('./results/')
    resultFileName = './results/RESULT_STOP_LIST.csv'
    routeDict = GetRouteIDs(busFileName)
    stopID = 1
    allRecords = []
    for routeID in routeDict.keys():
        fileName = './route_stop/route_stop_od'+str(routeID)+'.txt'
        routeStopsOD = ReadRouteStop(fileName)
        num_stops = len(routeStopsOD)
        stopIds = range(stopID,num_stops+stopID)
        stopID = num_stops+stopID
        routeIds = [routeID for tmp in range(num_stops)]
        directions = [0 for tmp in range(num_stops)]
        sequences = range(1,1+num_stops)
        lngs = [item[0] for item in routeStopsOD]
        lats = [item[1] for item in routeStopsOD]
        allRecords.extend(zip(stopIds,routeIds,directions,sequences,lngs,lats))

        
        fileName = './route_stop/route_stop_do'+str(routeID)+'.txt'
        routeStopsDO = ReadRouteStop(fileName)
        num_stops = len(routeStopsDO)
        stopIds = range(stopID,num_stops+stopID)
        stopID = num_stops+stopID
        routeIds = [routeID for tmp in range(num_stops)]
        directions = [1 for tmp in range(num_stops)]
        sequences = range(1,1+num_stops)
        lngs = [item[0] for item in routeStopsDO]
        lats = [item[1] for item in routeStopsDO]
        allRecords.extend(zip(stopIds,routeIds,directions,sequences,lngs,lats))

    OutputLists(allRecords,resultFileName)
    
    return True

def SubmitBusStopProject(busFileName):
##    (stop_id, route_id, direction, sequence, lng, lat)
##    1,R1, 0, 1, 112.032, 30.042
    if not os.path.exists('./results/'):
        os.makedirs('./results/')
    resultFileName = './results/RESULT_STOP_LIST_Project.csv'
    routeDict = GetRouteIDs(busFileName)
    stopID = 1
    allRecords = []
    for routeID in routeDict.keys():
        fileName = './route_stop/route_stop_od'+str(routeID)+'.txt'
        routeStopsOD = ReadRouteStop(fileName,False)
        num_stops = len(routeStopsOD)
        stopIds = range(stopID,num_stops+stopID)
        stopID = num_stops+stopID
        routeIds = [routeID for tmp in range(num_stops)]
        directions = [1 for tmp in range(num_stops)]
        sequences = range(1,1+num_stops)
        lngs = [item[0] for item in routeStopsOD]
        lats = [item[1] for item in routeStopsOD]
        allRecords.extend(zip(stopIds,routeIds,directions,sequences,lngs,lats))

        
        fileName = './route_stop/route_stop_do'+str(routeID)+'.txt'
        routeStopsDO = ReadRouteStop(fileName,False)
        num_stops = len(routeStopsDO)
        stopIds = range(stopID,num_stops+stopID)
        stopID = num_stops+stopID
        routeIds = [routeID for tmp in range(num_stops)]
        directions = [-1 for tmp in range(num_stops)]
        sequences = range(1,1+num_stops)
        lngs = [item[0] for item in routeStopsDO]
        lats = [item[1] for item in routeStopsDO]
        allRecords.extend(zip(stopIds,routeIds,directions,sequences,lngs,lats))

    OutputLists(allRecords,resultFileName)
    
    return allRecords
#--------------------------------------------------------------------


#------------------------Esimate alighitng -----------------------------
def BuildSpatialIndex(busFileName,resultFile,stops):
    routeDict = GetRouteIDs(busFileName)
    spatialDict = {}
    p = rtree.index.Property()
    p.dimension = 2
    
    for routeID in routeDict.keys():
        spatialDict['{0}_{1}'.format(routeID,1)] = rtree.index.Index(properties=p)
        spatialDict['{0}_{1}'.format(routeID,-1)] = rtree.index.Index(properties=p)

    
    for item in stops:
        stopID = item[0]
        routeID = item[1]
        direction = item[2]
        sequence = item[3]
        x = item[4]
        y = item[5]
        spatialDict['{0}_{1}'.format(routeID,direction)].add(stopID,(x,y))


    return spatialDict

def BuildTransferTable(busFileName,resultFile):
    
    if not os.path.exists('./bus_transfer/'):
        os.makedirs('./bus_transfer/')
    
    stops = ReadAllBusStops(resultFile)
    spatialDict = BuildSpatialIndex(busFileName,resultFile,stops)
    finished = 0
    routeDict = GetRouteIDs(busFileName)

    transferTable = []
    for item in stops:
        stopID = item[0]
        routeID = item[1]
        direction = item[2]
        sequence = item[3]
        x = item[4]
        y = item[5]
        
        finished = finished+1
        if (finished % 100)==0:
            print finished
        for transferRouteID in routeDict.keys():
            key = "{0}_{1}".format(transferRouteID, 1)
            idx = spatialDict[key]
            nearIdx = list(idx.nearest((x,y)))[0]
            transferTable.append([stopID,routeID,direction,sequence,transferRouteID,1,nearIdx])
            
            key = "{0}_{1}".format(transferRouteID, -1)
            idx = spatialDict[key]
            nearIdx = list(idx.nearest((x,y)))[0]
            transferTable.append([stopID,routeID,direction,sequence,transferRouteID,-1,nearIdx])
    fileName = './bus_transfer/transfer_table.txt'
    OutputLists(transferTable,fileName)
    return True

def BuildTransferTable_m(stops,routeDict,start,end,busFileName,resultFile,index):
    #stops = ReadAllBusStops(resultFile)
    
    spatialDict = BuildSpatialIndex(busFileName,resultFile,stops)
    #routeDict = GetRouteIDs(busFileName)
    
##    global transferTableMain
##    global lock
    
    
    transferTable = [[1,1,1,1] for tmp in range(0,(end-start)*2*len(routeDict.keys()))]
    
    finished = 0
    tableIndex = 0
    for i in range(start,end):
        finished = finished+1
        if (finished % 1000)==0:
            print float(finished)/(end-start)
            tmpFile = './tmpfile/finish'+str(index)+'_'+str(float(finished)/(end-start))+'.txt'
            open(tmpFile, 'a').close()
        
        
        item = stops[i]
        stopID = item[0]
        routeID = item[1]
        direction = item[2]
        sequence = item[3]
        x = item[4]
        y = item[5]
        
        #finished = finished+1
        #if (finished % 100)==0:
        #    print finished
        for transferRouteID in routeDict.keys():
            key = "{0}_{1}".format(transferRouteID, 1)
            idx = spatialDict[key]
            nearIdx = list(idx.nearest((x,y)))[0]
            #transferTable.append([stopID,routeID,direction,sequence,transferRouteID,1,nearIdx])
            transferTable[tableIndex] = [stopID,transferRouteID,1,nearIdx]
            tableIndex = tableIndex+1
            
            key = "{0}_{1}".format(transferRouteID, -1)
            idx = spatialDict[key]
            nearIdx = list(idx.nearest((x,y)))[0]
            #transferTable.append([stopID,routeID,direction,sequence,transferRouteID,-1,nearIdx])
            transferTable[tableIndex] = [stopID,transferRouteID,1,nearIdx]
            tableIndex = tableIndex+1
            
    print tableIndex

    fileName = './bus_transfer/transfer_table_join_'+str(index)+'.txt'
    OutputLists(transferTable,fileName)
##    lock.acquire()
##    transferTableMain.extend(transferTable)
##    lock.release()
    
    return transferTable



def ReadTransferTable(fileName):
    transferDict = {}
    idList = []
    with open(fileName,'r') as f:
        for line in f:
            tmpList = [int(x) for x in line.split(',')]
            stopID = tmpList[0]
            if not (stopID in idList):
                idList.append(stopID)
                transferDict[stopID] = {}
            
            transferRouteID = tmpList[4]
            transferRouteDirection = tmpList[5]
            nearIdx = tmpList[6]
            
            key = "{0}_{1}".format(transferRouteID, transferRouteDirection)
            transferDict[stopID][key] = nearIdx
    
    
    
    return transferDict

        
def ReadAllBusStops(resultFile):
    
    stops = []
    
    with open(resultFile, 'r') as f:
        for line in f:
            tmpList = line.split(',')
            stopID = int(tmpList[0])
            routeID = int(tmpList[1])
            direction = int(tmpList[2])
            sequence = int(tmpList[3])
            x = float(tmpList[4])
            y = float(tmpList[5])
            stops.append([stopID,routeID,direction,sequence,x,y])
            
    return stops


def MathGPSToBusStop(busFileName,resultFile):
    if not os.path.exists('./bus_match_stop/'):
        os.makedirs('./bus_match_stop/')
        
    stops = ReadAllBusStops(resultFile)
    spatialDict = BuildSpatialIndex(busFileName,resultFile,stops)
    
    routeDict = GetRouteIDs(busFileName)
    
    for routeID in routeDict.keys():
        for busID in routeDict[routeID]:
            fileName = './bus_match_stop/bus_stop_'+str(busID)+'.txt'
            if os.path.isfile(fileName):
                continue
            print routeID,busID
            busDirection = ReadBusDirection(busID)
            allGPS = GetProjectedGPS(busID)
            nearBusStop = []
            for i in range(len(busDirection)):
                x = allGPS[i][2]
                y = allGPS[i][3]
                direction = busDirection[i]
                key = "{0}_{1}".format(routeID, direction)
                idx = spatialDict[key]
                nearBusStopId = list(idx.nearest((x,y)))[0]
                nearBusStop.append(nearBusStopId)
            fileName = './bus_match_stop/bus_stop_'+str(busID)+'.txt'
            OutputSimpleList(nearBusStop,fileName)
            
    return True

def ReadNearBusStop(busID):
    fileName = './bus_match_stop/bus_stop_'+str(busID)+'.txt'

    f = open(fileName, 'r')
    lines = f.readlines()
    f.close()
    
    nearBusStop = [int(x) for x in lines]
    
    return nearBusStop


def GetAFCWithBusStop(busID):
    
    
    busAfcData = ReadAFCByBus(busID)
    afcTime = [x[2]*1000000+x[3] for x in busAfcData]

    allGPS = GetProjectedGPS(busID)
    nearBusStop = ReadNearBusStop(busID)

    if len(allGPS)==0:
        return []
    busTime = [x[0]*1000000+x[1] for x in allGPS]
    
    p = rtree.index.Property()
    p.dimension = 2
    idx = rtree.index.Index(properties=p)
    
    for i in range(len(busTime)):
        idx.add(i, (busTime[i],0))

    afcBusStop = []
    for aTime in afcTime:
        busTimeID = list(idx.nearest((aTime,0)))[0]
        afcBusStop.append(nearBusStop[busTimeID])
        
    return afcBusStop

def JudgeAFCBoarding(busFileName):
    if not os.path.exists('./afc_bus_stop/'):
        os.makedirs('./afc_bus_stop/')
    
    routeDict = GetRouteIDs(busFileName)
    
    for routeID in routeDict.keys():
        for busID in routeDict[routeID]:
            print routeID, busID
            fileName = './afc_bus_stop/afc_bus_stop_'+str(busID)+'.txt'
            afcBusStop = GetAFCWithBusStop(busID)
            OutputSimpleList(afcBusStop,fileName)
            
    return True

def JudgeAFCBoarding_m(busFileName,num_process,index):
    if not os.path.exists('./afc_bus_stop/'):
        os.makedirs('./afc_bus_stop/')
    
    routeDict = GetRouteIDs(busFileName)
    routeKeys = routeDict.keys()
    numRoutes = len(routeKeys)
    numEachPro = numRoutes/num_process
    start = index*numEachPro
    end = (index+1)*numEachPro
    if index==num_process-1:
        end = numRoutes
    
    for i in range(start,end):
        routeID = routeKeys[i]
        for busID in routeDict[routeID]:
            print routeID, busID
            fileName = './afc_bus_stop/afc_bus_stop_'+str(busID)+'.txt'
            afcBusStop = GetAFCWithBusStop(busID)
            OutputSimpleList(afcBusStop,fileName)

    return True

def ReadAfcBusStop(busID):
    fileName = './afc_bus_stop/afc_bus_stop_'+str(busID)+'.txt'

    f = open(fileName, 'r')
    lines = f.readlines()
    f.close()
    
    afcBusStop = [int(x) for x in lines]
    
    return afcBusStop

def MergeAfcData(busFileName):
    routeDict = GetRouteIDs(busFileName)
    allAfcData = []
    for routeID in routeDict.keys():
        for busID in routeDict[routeID]:
            print routeID,busID
            afcBusStop = ReadAfcBusStop(busID)
            busAfcData = ReadAFCByBus(busID)
            for i in range(0,len(afcBusStop)):
                busAfcData[i].append(afcBusStop[i])
            allAfcData.extend(busAfcData)
    return allAfcData
            

def ReorganizeAFCByWithBusStop(busFileName):

    if not os.path.exists('./afc_cardid/'):
        os.makedirs('./afc_cardid/')
    
    afcData = MergeAfcData(busFileName)
    cardIds  = list(set([tmp[1] for tmp in afcData]))
    afcDict = {}
    for cardId in cardIds:
        afcDict[cardId] = []

    for item in afcData:
        afcDict[item[1]].append(item)

    for cardId in cardIds:
        fileName = './afc_cardid/card_afc_'+str(cardId)+'.txt'
        if os.path.isfile(fileName):
                continue
        cardAfcData = afcDict[cardId]
        
        index = [x[2]*1000000+x[3] for x in cardAfcData]
        sortCardAfcData = [x for (y,x) in sorted(zip(index,cardAfcData))]
        
        OutputLists(sortCardAfcData,fileName)
    
    return True

def ReorganizeAFCByWithBusStop_m(afcDict,cardIds,busFileName,num_process,index):

    if not os.path.exists('./afc_cardid/'):
        os.makedirs('./afc_cardid/')
##    
##    afcData = MergeAfcData(busFileName)
##    cardIds  = list(set([tmp[1] for tmp in afcData]))
##    afcDict = {}
##    for cardId in cardIds:
##        afcDict[cardId] = []
##
##    for item in afcData:
##        afcDict[item[1]].append(item)


    numItems = len(cardIds)
    numEachPro = numItems/num_process
    start = index*numEachPro
    end = (index+1)*numEachPro
    if index==num_process-1:
        end = numItems
    print start,end
    for i in range(start,end):
        cardId = cardIds[i]
        fileName = './afc_cardid/card_afc_'+str(cardId)+'.txt'
        if os.path.isfile(fileName):
                continue
        cardAfcData = afcDict[cardId]
        
        index = [x[2]*1000000+x[3] for x in cardAfcData]
        sortCardAfcData = [x for (y,x) in sorted(zip(index,cardAfcData))]
        
        OutputLists(sortCardAfcData,fileName)
    
    return True

    



def ReadCardAFC(fileName):
    busAfcData = []
    with open(fileName, 'rb') as f:
        for line in f:
            tmpList = [int(x) for x in line.split(',')]
            busAfcData.append(tmpList)
    return busAfcData


def EstimateAFCAlighting():
    resultFileName = './results/RESULT_STOP_LIST_Project.csv'
    stops = ReadAllBusStops(resultFileName)
    print 'constructing spatial index'
    spatialDict = BuildSpatialIndex(busFileName,resultFileName,stops)

    print 'estimate alighting'
    alightStops = []


    afcData = MergeAfcData(busFileName)
    cardIds  = list(set([tmp[1] for tmp in afcData]))
    afcDict = {}
    for cardId in cardIds:
        afcDict[cardId] = []
        
    for item in afcData:
        afcDict[item[1]].append(item)

    
    for i in range(len(cardIds)):
        if (i %100000) == 0:
            print float(i)/len(cardIds)
            
        cardId = cardIds[i]
        cardAfcData = afcDict[cardId]
        
        index = [x[2]*1000000+x[3] for x in cardAfcData]
        busAfcData = [x for (y,x) in sorted(zip(index,cardAfcData))]
        
        if len(busAfcData)<2:
            continue
        
        for i in range(0,len(busAfcData)):
            guid = busAfcData[i][0]
            firstStopID = busAfcData[i][-1]
            firstRouteID = stops[firstStopID-1][1]
            firstRouteDirection = stops[firstStopID-1][2]
            
            
            if i+1!=len(busAfcData):
                nextStopID = busAfcData[i+1][-1]
            else:
                nextStopID = busAfcData[0][-1]
            
            #nextRouteID = stops[nextStopID-1][1]
            #nextRouteDirection = stops[nextStopID-1][2]
            x = stops[nextStopID-1][4]
            y = stops[nextStopID-1][5]
            
            key = "{0}_{1}".format(firstRouteID, firstRouteDirection)
            idx = spatialDict[key]
            alightStopID = list(idx.nearest((x,y)))[0]

            x = stops[alightStopID-1][4]
            y = stops[alightStopID-1][5]
            
            alightStops.append([guid,x,y])
    
    fileName = './results/RESULT_ALIGHT_LIST_Project.csv'

    OutputLists(alightStops,fileName)

def DoStat():
    if not os.path.exists('./stats_pair/'):
        os.makedirs('./stats_pair/')
        
    resultFileName = './results/RESULT_STOP_LIST_Project.csv'
    stops = ReadAllBusStops(resultFileName)
    #print 'constructing spatial index'
    #spatialDict = BuildSpatialIndex(busFileName,resultFileName,stops)

    print 'estimate alighting'
    alightStops = []


    afcData = MergeAfcData(busFileName)
    cardIds  = list(set([tmp[1] for tmp in afcData]))
    afcDict = {}
    for cardId in cardIds:
        afcDict[cardId] = []
        
    for item in afcData:
        afcDict[item[1]].append(item)

    pairs = []
    for i in range(len(cardIds)):
        if (i %100000) == 0:
            print float(i)/len(cardIds)
            
        cardId = cardIds[i]
        cardAfcData = afcDict[cardId]
        
        index = [x[2]*1000000+x[3] for x in cardAfcData]
        busAfcData = [x for (y,x) in sorted(zip(index,cardAfcData))]

        if len(busAfcData)<2:
                continue
        
        for i in range(0,len(busAfcData)):
            firstStopID = busAfcData[i][-1]
            if i+1!=len(busAfcData):
                nextStopID = busAfcData[i+1][-1]
            else:
                nextStopID = busAfcData[0][-1]
        
            pairs.append([firstStopID,nextStopID])

    pairDict = {}
    for i in range(len(pairs)):
        if (i%10000)==0:
            print float(i)/len(pairs)
        item  = pairs[i]
        if not (item[0] in pairDict.keys()):
            #print item
            pairDict[item[0]] = []
            pairDict[item[0]].append(item[1])
        else:
            pairDict[item[0]].append(item[1])

    statisResults = []
    for key in pairDict.keys():
        tmpList = pairDict[key]
        tmpCount = Counter(tmpList)
        #print key
        statisResults.append([key,tmpCount.most_common(1)[0][0]])
        
    
    fileName = './stats_pair/pairs.txt'
    OutputLists(statisResults,fileName)

    return True

def ReadStatistic()
    fileName = './stats_pair/pairs.txt'
    statPairs = {}
    with open(fileName,'r') as f:
        for line in f:
            tmpList = [int(x) for x in line.split(',')]
            statPairs[tmpList[0]] = tmpList[1]
    
    return statPairs
            

def GuessUnknow():#UNFINISHED~~~~~~~~~~~~~~~~~~~~~~~~~
    
    resultFileName = './results/RESULT_STOP_LIST_Project.csv'
    stops = ReadAllBusStops(resultFileName)
    print 'constructing spatial index'
    spatialDict = BuildSpatialIndex(busFileName,resultFileName,stops)

    print 'estimate alighting'
    alightStops = []

    statPairs = ReadStatistic()
    
    
    afcData = MergeAfcData(busFileName)
    cardIds  = list(set([tmp[1] for tmp in afcData]))
    afcDict = {}
    for cardId in cardIds:
        afcDict[cardId] = []
        
    for item in afcData:
        afcDict[item[1]].append(item)
    pairFileName = './stats_pair/pairs.txt'
    pairs = ReadPairs()
    for i in range(len(cardIds)):
        if (i %100000) == 0:
            print float(i)/len(cardIds)
            
        cardId = cardIds[i]
        cardAfcData = afcDict[cardId]
        
        index = [x[2]*1000000+x[3] for x in cardAfcData]
        busAfcData = [x for (y,x) in sorted(zip(index,cardAfcData))]
        
        if len(busAfcData)>=2:
            continue
        
        guid = busAfcData[0][0]
        firstStopID = busAfcData[0][-1]
        if firstStopID in statPairs.keys():


        firstRouteID = stops[firstStopID-1][1]
        
        

    OutputLists(alightStops,fileName)


def SubmitAlighting():
    fileName = './results/RESULT_ALIGHT_LIST_Project.csv'
    guids = []
    xys = []
    
    with open(fileName,'r') as f:
        for line in f:
            tmpList = line.split(',')
            guids.append(int(tmpList[0]))
            xys.append([float(tmpList[1]),float(tmpList[2])])

    lnglats = ProjectCoors(xys,True)

    lngs = [item[0] for item in lnglats]
    lats = [item[1] for item in lnglats]

    alightStops = zip(guids,lngs,lats)
    fileName = './results/RESULT_ALIGHT_LIST.csv'
    OutputLists(alightStops,fileName)
    
    
    
#--------------------------------------------------------------------



#----------General Geomtric Functions-------------------
def Magnitude(p1, p2):
    vect_x = p2.x - p1.x
    vect_y = p2.y - p1.y
    return sqrt(vect_x**2 + vect_y**2)

def DisToLine(point, line_start, line_end):
    endPoint,notFallInLine = IntersectPointToLine(point, line_start, line_end)
    
    return Magnitude(point,endPoint),notFallInLine

def IntersectPointToLine(point, line_start, line_end):
    line_magnitude =  Magnitude(line_end, line_start)
    u = ((point.x - line_start.x) * (line_end.x - line_start.x) +
         (point.y - line_start.y) * (line_end.y - line_start.y)) \
         / (line_magnitude ** 2)

    # closest point does not fall within the line segment, 
    # take the shorter distance to an endpoint
    notFallInLine = False
    if u < 0.00001 or u > 1:
        ix = Magnitude(point, line_start)
        iy = Magnitude(point, line_end)
        notFallInLine = True
        if ix > iy:
            return line_end,notFallInLine
        else:
            return line_start,notFallInLine
    else:
        ix = line_start.x + u * (line_end.x - line_start.x)
        iy = line_start.y + u * (line_end.y - line_start.y)
        return Point(ix, iy),notFallInLine

def GetAngle(p1,p2):
    vect_x = p2.x - p1.x
    vect_y = p2.y - p1.y
    return atan2(vect_y, vect_x)/math.pi*180


def GetBoundingBox(points):
    xmin = 1e10
    xmax = -1e10
    ymin = xmin
    ymax = xmax
    for item in points:
        if item[0]<xmin:
            xmin = item[0]
        if item[0]>xmax:
            xmax = item[0]
        if item[1]<ymin:
            ymin = item[1]
        if item[1]>ymax:
            ymax = item[1]
    return [(xmin,ymin),(xmax,ymax)]
    

#-------------------------------------------------------



#--------------------Use for Test-----------------------

def OutputNodeSeq(nodeSequence,nodes,fileName):
    with open(fileName, 'w') as f:
        for nodeID in nodeSequence:
            f.write(str(nodeID)+','+','.join([str(x)for x in nodes[nodeID]])+'\n')

def CheckRoute(routeID):
    if not os.path.exists('./tmp/'):
        os.makedirs('./tmp/')
    
    allODGPS = GetRouteOD(routeID)
    fileName = './tmp/route_ods'+str(routeID)+'.txt'
    (allODGPS,fileName)
    


#-------------------------------------------------------


#---------------------Multiprocessing-----------------------------
def ParaJudgeAFCAlighting():
    
    num_process = 5
    thread = []
    for i in range(0,num_process):
        thread.append(multiprocessing.Process(target = JudgeAFCBoarding_m ,
                                       args = (busFileName,num_process,i)))
        
    for i in range(0,num_process):
        print thread[i]
        thread[i].start()
    for i in range(0,num_process):
        print thread[i]
        thread[i].join()


def ParaReorganizeAFCByWithBusStop():
    
    afcData = MergeAfcData(busFileName)
    cardIds  = list(set([tmp[1] for tmp in afcData]))
    afcDict = {}
    for cardId in cardIds:
        afcDict[cardId] = []

    for item in afcData:
        afcDict[item[1]].append(item)

    del afcData

    num_process = 5
    thread = []

    for i in range(0,num_process):
        thread.append(multiprocessing.Process(target = ReorganizeAFCByWithBusStop_m ,
                                       args = (afcDict,cardIds,busFileName,num_process,i)))
        
    for i in range(0,num_process):
        print thread[i]
        thread[i].start()
    del afcDict
    for i in range(0,num_process):
        print thread[i]
        thread[i].join()
    
    

    
def ParaBusTransfer():
    
    if not os.path.exists('./bus_transfer/'):
        os.makedirs('./bus_transfer/')
        
    transferTableMain = []
    lock = multiprocessing.Lock()
    
    stops = ReadAllBusStops(resultFileName)
    #spatialDict = BuildSpatialIndex(busFileName,resultFileName,stops)
    finished = 0
    routeDict = GetRouteIDs(busFileName)
    
    
    threadNum = 4
    numEachTread = len(stops)/threadNum

    thread = []
    for i in range(0,threadNum):
        start = i*numEachTread
        end = (i+1)*numEachTread
        print start,end
        if i == threadNum-1:
            end = len(stops)
        thread.append(multiprocessing.Process(target = BuildTransferTable_m ,
                                       args = (stops,routeDict,start,end,busFileName,resultFileName,i)))

    for i in range(0,threadNum):
        print thread[i]
        thread[i].start()
    
    for i in range(0,threadNum):
        print thread[i]
        thread[i].join()
    
    fileName = './bus_transfer/transfer_table_join.txt'
    OutputLists(transferTableMain,fileName)

#-------------------------------------------------------------------------
#



##################################Main########################################

#-------------------------preprocess bus gps----------------
#ReOrganizeGPS(gpsFileName, busFileName, testOn)
#CleanBUSGPS(busFileName)
#------------------------------------------------------------


#---------------Build graph from geojson------------------------
nodeFile = './road/nodes.txt'
edgeFile = './road/edges.txt'
#nodes,edges = GeoJsonToGraph(geoFileName)
#OutputLists(nodes,nodeFile)
#OutputLists(edges,edgeFile)


#project to local coords
#nodesPro = ProjectNodes(nodes)
#nodesProFile = './road/nodes_pro.txt'
#OutputLists(nodesPro,nodesProFile)


nodeFileName = './road/nodes_pro.txt'
edgeFileName = './road/edges.txt'

#GetNodesBoundingBox(nodeFileName)

#-------------------------------------------------------------



#------------------Get Route Origin and Destination-----------------
###testing~~~~~~~~~~~~~~~~~~~
###routeDict = GetRouteIDs(busFileName)
##busID = 1
##routeID = 2
###allODGPS = GetRouteOD(routeID)
###allODGPS = OutputRouteOD(routeID)
##lonlats = []
#routeDict = GetRouteIDs(busFileName)


#GetALLRoutesOD(busFileName)
#FixAllRoutesOD(busFileName)
#ods = GetRouteODCenters(routeID)
#GetAllBusTrip(busFileName)

#-------------------------------------------------------------


#------------------Path Inference------------------------------

#RouteInference(nodeFileName,edgeFileName,busFileName)
#ReorganizeAFCByBus(afcFileName,busFileName)
#ReorganizeAFCByCardID(afcFileName)
#--------------------------------------------------------------



#-----------------Bus Stop Inference---------------------------

#JudgeDirectionAllBus(busFileName)
#GetAFCCountAllBus(busFileName)
#AggregateBusStop(busFileName)
#OutputAllRoute(busFileName)
#OutputAllIntervalRoute(busFileName)
#SubmitBusStop(busFileName)
#SubmitBusStopProject(busFileName)
#SubmitIntervalBusStop(busFileName)
#--------------------------------------------------------------


#-----------------Estimating Alighting--------------------------

resultFileName = './results/RESULT_STOP_LIST_Project.csv'
#BuildTransferTable(busFileName,resultFileName)
#MathGPSToBusStop(busFileName,resultFileName)
#JudgeAFCBoarding(busFileName)
#ReorganizeAFCByWithBusStop(busFileName)

#EstimateAFCAlighting()
#SubmitAlighting()
#DoStat()


#----------------------MultiProcessing---------------------------
if __name__ == "__main__":
    dump = 0
    #ParaBusTransfer()
    #ParaJudgeAFCAlighting()
    #ParaReorganizeAFCByWithBusStop()


##################################END########################################









