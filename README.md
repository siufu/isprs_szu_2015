# isprs_szu_2015

This program is organized in purely procedural style. No ancillary data is used. 100+ functions have been coded. But not all functions has been used finally. Some were deprecated after new methods were found, but codes were still preserved. 

The program is dependent on several 3rd party libs as listed below:
- **pyproj**: provides function to project among different coordinate system
- **rtree**: use r-tree spatial index to accelerate 
- **sklearn**: use KMeans, MiniBatchKMeans, and DBScan for clustering
- **networkx**: graph/topology is built based on it

You could find the main entrance from line 2820.
Here I will brief the rough ideas and explain key functions.

##Part 1. Preprocess Bus GPS
	Divide bus gps data (GPS_DATA.csv) into each bus using busid. Sort gps fixes by time, remove noise, and project to UTM49N
1. **ReOrganizeGPS**(*gpsFileName, busFileName, testOn*)
	divide all bus gps data into each bus, sort gps by time, and store in file
2. **CleanBUSGPS**(*busFileName*)
	remove noise, and project to UTM49N
	
##Part 2. Build Graph/Topology from network geojson
	Topology between different road links is built in this part.
1. nodes,edges = **GeoJsonToGraph**(*geoFileName*)
	Reorganize network geojson file to two files, one stores nodes, and the other stores edges. Each node has a unique id. If two edges share the same ends, node id will be the same. Graph/Topology can thus be built based on these two files.

##Part 3. Get Route Origins and Destinations
	The first and last bus stops are estimated based on the first gps point with AFC information.
1. **GetALLRoutesOD**(*busFileName*)
	Get first gps points of all buses in the same route. Do clustering to get two cluster centers which deviate most.
2. **GetAllBusTrip**(*busFileName*)
	Use the first and last bus stop to divide bus gps data into each single trip.
	
##Part 4. Path Inference
	Use all gps data to infer route path.
1. **RouteInference**(*nodeFileName,edgeFileName,busFileName*)
	Build road network. For a single route, original impedence of each road link is the length itself. Impedence is then reduced if gps points are found nearby. Path can thus be derived when estimate shortest route from origin to destination.

##Part 5. Bus Stop Inference
	Estimate bus stops.
1. **JudgeDirectionAllBus**(*busFileName*)
	For each gps fix, judge which direction it is on.
2. **GetAFCCountAllBus**(*busFileName*)
	Count AFCs to each gps fix if it is within a certain time tolerance. 
3. **AggregateBusStop**(*busFileName*)
	Discretize route path. Using linear reference to accelerate speed. Snap gps fix to route path points as well as its AFC counts.
4. **OutputAllRoute**(*busFileName*)
	Apply clustering to the discretized route path (with afc counts). Get bus stops based on the cluster centers.
5. **SubmitBusStop**(*busFileName*)
	Project bus stop to longitude and latitude. Combine and submit file.

	
##Part 6. Estimating Alighting
	Estimate alighting based on the next boarding
1. **MathGPSToBusStop**(*busFileName,resultFileName*)
	Match each gps fix to a bus stop
2. **JudgeAFCBoarding**(*busFileName*)
	Get the boarding bus stop of each AFC data.
3. **JudgeAFCAlighting**()
	Estimate the alighting bus stop based on the next boarding bus stop
4. **SubmitAlighting**()
	Project alighting bus stop into longitude and latitude. Combine and submit file.

To accelerate the speed, some part has been modified using multiprocessing. 
