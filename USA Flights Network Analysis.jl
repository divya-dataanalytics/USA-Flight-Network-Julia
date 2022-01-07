# display setting
ENV["COLUMNS"] = 200
ENV["LINES"] = 5

# include packages
using CSV, DataFrames, HTTP
using WebIO
using Query
using Graphs, GraphPlot, SimpleWeightedGraphs
using Colors
using Interact
using VegaLite, VegaDatasets

# function to read csv dataset into dataframe
read_csv_df(url) = DataFrame(CSV.File(HTTP.get(url).body, header = 0))

# read routes dataset
routes_url = "https://raw.githubusercontent.com/jpatokal/openflights/master/data/routes.dat"
routes_df = read_csv_df(routes_url)
println(nrow(routes_df))

# assign column names to dataframe
colnames = ["Airline_IATA","Airline_ID","Source_IATA", "Source_ID","Dest_IATA", "Dest_ID", "Codeshare", "Stops","Equipment"  ]
rename!(routes_df, Symbol.(colnames))

# drop columns that are not required
routes_df = select(routes_df, Not([:Codeshare, :Equipment]))

# filter by number of stops 0 for direct flights
routes_df = routes_df[( routes_df.Stops .== 0 ), :]

# read airport dataset
airports_url = "https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat"
airports_df = read_csv_df(airports_url)
println(nrow(airports_df))

# assign column names to dataframe
colnames = ["Airport_ID", "Name", "City", "Country", "IATA", "ICAO", "latitude", "longitude", 
            "Altitude", "Timezone", "DST", "Tzdatabase_time", "Type", "Source"]
rename!(airports_df, Symbol.(colnames))

# drop columns that are not required
airports_df = select(airports_df, Not([:ICAO, :Altitude, :Timezone, :DST, :Tzdatabase_time, :Type, :Source]))
first(airports_df, 5)

# read airline dataset
airlines_url = "https://raw.githubusercontent.com/jpatokal/openflights/master/data/airlines.dat"
airlines = read_csv_df(airlines_url)

# assign column names to dataframe
colnames = ["Airline_ID", "Name", "Alias", "IATA", "ICAO", "Callsign", "Country", "Active"             ]
rename!(airlines, Symbol.(colnames))

# drop rows with null values
airlines_df = airlines[completecases(airlines), :]
#airlines_df = airlines[(airlines.Country .== "United States"), :]

# filter USA airports and remove rows with null ("\\N") IATA 
airports_usa = airports_df[(airports_df.Country .== "United States") .& (airports_df.IATA .!= "\\N" ), :]

# filter usa routes 
routes_usa = filter(row -> row.Source_IATA ∈ airports_usa.IATA, routes_df) #filter source to include only US airports
routes_usa = filter(row -> row.Dest_IATA   ∈ airports_usa.IATA, routes_usa) #filter dest to include only US airports

# combine same route flights and get count/frequency
flight_freq = DataFrame(routes_usa |> @groupby({_.Source_IATA, _.Dest_IATA, _.Source_ID, _.Dest_ID}) |> @map({Source_IATA=key(_)[1], Dest_IATA=key(_)[2],Source_ID=key(_)[3], Dest_ID=key(_)[4],  
                  count=length(_)}))

sort!(flight_freq, [:Source_IATA, :Dest_IATA])

function generategraph(df, src, dst, graphtype)
    
    # get a list of nodes from dataframe   
    nodes = sort(unique([df[!, src] ; df[!, dst]]))
    nodes_len = size(nodes)[1]
    
    # assign node numbers
    nodes_int = [x for x in 1:nodes_len]
    nodes_dict = Dict(zip(nodes, nodes_int))
    
    # create directed/undirected graph based on input
    if graphtype == "directed"
        g = SimpleWeightedDiGraph(nodes_len)
    else
        g = SimpleWeightedGraph(nodes_len)
    end
        
    # add source, destination, weights & create edges
    ew = Int[] 
    for i in 1:nrow(df)
        w = df.count[i]
        push!(ew, w)
        source = nodes_dict[df[!, src][i]]
        destination = nodes_dict[df[!, dst][i]]
        add_edge!(g, source, destination, w)
    end
    
return g, nodes
end

# get nodes and graph for initial USA flight network and plot
g1, nodes1 = generategraph(flight_freq, "Source_IATA", "Dest_IATA", "directed")
println("The number of edges in network is ", ne(g1))
println("The number of nodes in network is ", nv(g1))
printstyled("\n\t\t\tDirected Weighted Graph"; color=:bold)
gplot(g1, nodefillc = "green", linetype="curve", EDGELINEWIDTH = 0.5) # plot default spring layout

pip install notebook


printstyled("\n\t\t\t\t\t\tDirected Weighted Graph"; color=:bold)
@manipulate for frequency = slider(1:1:20, show_value=true, label = "Number of flights")
    filter_flights = sort(flight_freq[(flight_freq.count .>= frequency[]), :]) # filter df by flight frequency chosen
    g2, nodes2 = generategraph(filter_flights, "Source_IATA", "Dest_IATA", "directed") 
    weights2 = weight.(edges(g2))
    gplot(g2, nodefillc = "green", edgelabel = weights2, nodelabel = nodes2) # plot default spring layout
end

# Use all airports and routes graph 
# assign node size based on outdegree
nodesize3 = [outdegree(g1, v) for v in Graphs.vertices(g1)]

# assign color based on node size
size3 = nodesize3/maximum(nodesize3)
nodefillc3 = [RGB(i*5,i^2,i/10) for i in size3]

printstyled("\n\t\tNodes color/size by Outdegree"; color=:bold)

# plot on random layout
gplot(g1, layout = random_layout, nodefillc = nodefillc3, NODESIZE = nodesize3/1000,  nodelabel = nodes1,
        NODELABELSIZE = nodesize3/20, EDGELINEWIDTH = weight.(edges(g1))/50)

# nodes with outdegree > 100 (airports that has over 100 destinations)
high_outdegree = findall(item -> item > 100, nodesize3)

printstyled("Airports with outdegree > 100\n\n"; color=:bold)
printstyled("Airport_IATA    Outdegree   Airport_Name\n", color = :bold)
for i in high_outdegree
    airport_name = reduce(vcat,[airports_usa[(airports_usa.IATA .== nodes1[i]) ,:].Name ])
    println("   ", nodes1[i],"\t\t  ", nodesize3[i] ,"       ", airport_name[])
end

# Use the all airports and routes graph 
# assign node size based on betweenness centrality
nodesize4 =  betweenness_centrality(g1)

# assign color based on node size
size4 = nodesize4/maximum(nodesize4)
nodefillc4 = nodefillc = [RGBA(0.3,i,0.8,i) for i in size4] #[RGB(0.5,i^2,i/10) for i in size4]

# define 3 shells for shell layout
nlist = Vector{Vector{Int}}(undef, 3) 
nlist[1] = 1:100 # first shell
nlist[2] = 101:300 # second shell
nlist[3] = 301:nv(g1) # third shell
locs_x, locs_y = shell_layout(g1, nlist)

printstyled("\n\t\tNodes color/size by betweenness centrality"; color=:bold)

# plot on shell layout
gplot(g1, locs_x, locs_y, nodefillc = nodefillc4, NODESIZE = nodesize4,  nodelabel = nodes1,
        NODELABELSIZE = nodesize4*30, EDGELINEWIDTH = weight.(edges(g1))/50)

# nodes with high betweenness centrality (centrality measure based on shortest paths)
high_bet = findall(item -> item > 0.08, nodesize4)

printstyled("Airports with betweenness centrality > 0.08\n\n"; color=:bold)
printstyled("Airport_IATA   betweenness\t Airport_Name\n", color = :bold)
for i in high_bet
    airport_name = reduce(vcat,[airports_usa[(airports_usa.IATA .== nodes1[i]) ,:].Name ])
    println("   ", nodes1[i],"\t       ", floor.(nodesize4[i], digits = 10), "      ", airport_name[])
end

# create a new dataframe to combine flights between same airports ( a->b  & b->a)
flights_undirected = DataFrame(Airport1 = String[], Airport2 = String[], count = Int64[])

for row in eachrow(flight_freq)
    src, dest, cnt = row.Source_IATA, row.Dest_IATA, row.count 
    df_flights = flight_freq[(flight_freq.Source_IATA .== dest) .& (flight_freq.Dest_IATA .== src), :]
    df_length = nrow(df_flights)
    
    if df_length == 1
        if isempty(flights_undirected[(flights_undirected.Airport2 .== src) .& (flights_undirected.Airport1 .== dest), :])
            push!(flights_undirected, [src  dest cnt + df_flights.count[]])
        end
    elseif df_length == 0
        push!(flights_undirected, [src  dest cnt])           
    end
end

sort!(flights_undirected, [:Airport1, :Airport2])

# get nodes and graph for USA flight undirected network and plot
g5, nodes5 = generategraph(flights_undirected, "Airport1", "Airport2", "undirected")
println("The number of edges in network is ", ne(g5))
println("The number of nodes in network is ", nv(g5))
printstyled("\n\t\t\tUndirected Weighted Graph"; color=:bold)
gplot(g5, linetype="curve", EDGELINEWIDTH = 0.5)

printstyled("\n\t\t\t\t\t\tUndirected Weighted Graph"; color=:bold)

@manipulate for frequency = slider(1:1:39, show_value=true, label = "Number of flights")
    filter_flights_undir = sort(flights_undirected[(flights_undirected.count .>= frequency[]), :])
    g6, nodes6 = generategraph(filter_flights_undir, "Airport1", "Airport2", "undirected")
    weights6 = weight.(edges(g6))
    gplot(g6, edgelabel = weights6, linetype="curve", nodelabel = nodes6) 
end

# import US map JSON dataset
us10m = dataset("us-10m")

# function to create a map view of flight network
function airportmap(airport_view)
    # plot size
    @vlplot(width = 800, height = 500) +
    
    # base map
    @vlplot(
        mark={
            :geoshape,
            fill=:white,
            stroke=:black
        },
        title = "Network by Source Airport - " * airports_usa[(airports_usa.IATA .== airport_view),:].Name[] ,
        data={
            values=us10m,
            format={
                type=:topojson,
                feature=:states
            }
        },
        projection={type=:albersUsa}) +
    
      # line marker connecting source and destination airports
    @vlplot(
        :rule,
        data=routes_usa,
        transform=[
            {filter={field=:Source_IATA,equal=airport_view}},
            {
                lookup=:Source_IATA,
                from={
                    data=airports_usa,
                    key=:IATA,
                    fields=["latitude", "longitude"]
                },
                as=["origin_latitude", "origin_longitude"]
            },
            {
                lookup=:Dest_IATA,
                from={
                    data=airports_usa,
                    key=:IATA,
                    fields=["latitude", "longitude"]
                },
                as=["dest_latitude", "dest_longitude"]
            }
        ],
        projection={type=:albersUsa},
        longitude="origin_longitude:q",
        latitude="origin_latitude:q",
        longitude2="dest_longitude:q",
        latitude2="dest_latitude:q",
        color={value=:lightblue}) +
    
    # point marker for destination aiports 
    @vlplot(
        data = routes_usa,
        mark = :point,
        transform=[
            {filter={field=:Source_IATA,equal=airport_view}},
            {
                lookup=:Source_IATA,
                from={
                    data=airports_usa,
                    key=:IATA,
                    fields=["latitude", "longitude"]
                }}],

        projection = {type=:albersUsa},
        longitude = "longitude:q",
        latitude="latitude:q",
        size={value=30},
        color={value=:green}) +
    
     # point marker for source aiport
    @vlplot(
        data = routes_usa,
        mark = :point,
        transform=[
            {filter={field=:Source_IATA,equal=airport_view}},
            {
                lookup=:Dest_IATA,
                from={
                    data=airports_usa,
                    key=:IATA,
                    fields=["latitude", "longitude"]
                }}],

        projection = {type=:albersUsa},
        longitude = "longitude:q",
        latitude="latitude:q",
        size={value=10},
        color={value=:red}) +
    
    # text marker for displaying destination aiport name
    @vlplot(
        data = routes_usa,
        mark={
            type=:text,
            dy=-10
        },
        transform=[
            {filter={field=:Source_IATA,equal=airport_view}},
            {
                lookup=:Dest_IATA,
                from={
                    data=airports_usa,
                    key=:IATA,
                    fields=["latitude", "IATA", "longitude"]
                }}],

        projection = {type=:albersUsa},
        longitude = "longitude:q",
        latitude= "latitude:q",
        size= {value=10},
        color= {value=:black},
        text = "IATA:n") +
    
     # text marker for displaying source aiport name
    @vlplot(
        data = routes_usa,
        mark={
            type=:text,
            dy=-10
        },
        transform=[
            {filter={field=:Source_IATA,equal=airport_view}},
            {
                lookup=:Source_IATA,
                from={
                    data=airports_usa,
                    key=:IATA,
                    fields=["latitude", "IATA", "longitude"]
                }}],

        projection = {type=:albersUsa},
        longitude = "longitude:q",
        latitude= "latitude:q",
        size= {value=10},
        color= {value=:black},
        text = "IATA:n") 
end

# dropdown widget (choose airport from dropdown to view network for different airport)
node_selected = dropdown(nodes1, show_value=true, label = "Select Source Airport", value = "DFW")
@manipulate for src_airport = node_selected
    airportmap(src_airport)  
end

global_clustering_coefficient(g1)

# function to create a map view of flight network
function airlinemap(airline_view, airline_name)
    # plot size
    @vlplot(width = 800, height = 500) +
    
    # base map
    @vlplot(
        mark={
            :geoshape,
            fill=:white,
            stroke=:black
        },
        title = "Network by Airlines - " * airline_name ,
        data={
            values=us10m,
            format={
                type=:topojson,
                feature=:states
            }
        },
        projection={type=:albersUsa}) +
    
    # point marker for aiports 
    @vlplot(
        data = routes_usa,
        mark = :point,
        transform=[
            {filter={field=:Airline_IATA,equal=airline_view}},
            {
                lookup=:Dest_IATA,
                from={
                    data=airports_usa,
                    key=:IATA,
                    fields=["latitude", "longitude"]
                }}],

        projection = {type=:albersUsa},
        longitude = "longitude:q",
        latitude="latitude:q",
        size={value=10},
        color={value=:red}) +
    
    # text marker for displaying destination aiport name
    @vlplot(
        data = routes_usa,
        mark={
            type=:text,
            dy=-10
        },
        transform=[
            {filter={field=:Airline_IATA,equal=airline_view}},
            {
                lookup=:Dest_IATA,
                from={
                    data=airports_usa,
                    key=:IATA,
                    fields=["latitude", "IATA", "longitude"]
                }}],

        projection = {type=:albersUsa},
        longitude = "longitude:q",
        latitude= "latitude:q",
        size= {value=10},
        color= {value=:black},
        text = "IATA:n") +
    
    # line marker connecting source and destination airports
    @vlplot(
        :rule,
        data=routes_usa,
        transform=[
            {filter={field=:Airline_IATA,equal=airline_view}},
            {
                lookup=:Source_IATA,
                from={
                    data=airports_usa,
                    key=:IATA,
                    fields=["latitude", "longitude"]
                },
                as=["origin_latitude", "origin_longitude"]
            },
            {
                lookup=:Dest_IATA,
                from={
                    data=airports_usa,
                    key=:IATA,
                    fields=["latitude", "longitude"]
                },
                as=["dest_latitude", "dest_longitude"]
            }
        ],
        projection={type=:albersUsa},
        longitude="origin_longitude:q",
        latitude="origin_latitude:q",
        longitude2="dest_longitude:q",
        latitude2="dest_latitude:q",
        color={value=:lightblue})
    
end

# list of airlines having flights
airlines_list = unique(routes_usa[!, "Airline_IATA"];)

# dict to get airlines name
airlines_dict = Dict()
for i in airlines_list
    airlines_name = airlines_df[(airlines_df.IATA .== i), :].Name
    if isempty(airlines_name)  
        airlines_dict[i] = i # K3 airlines - no airline map, display airline IATA
    elseif length(airlines_name) == 1
        airlines_dict[i] = airlines_name[] # DL, AA - unique name
    elseif length(airlines_name) > 1
        airlines_dict[i] = airlines_df[(airlines_df.IATA .== i), :].Name[1] # K5 - more than one name, display first
    end  
end

# interactive dropdown (choose airport from dropdown to view network for different ariline)
airline_selected = dropdown(airlines_list, show_value=true, label = "Select Airlines", value = "DL")
@manipulate for al = airline_selected
    airline_name = airlines_dict[al]
    airlinemap(al, airline_name)  
end

# Update weights to one to find shortest path
flights_airports = DataFrame(routes_usa |> @groupby({_.Source_IATA, _.Dest_IATA, _.Source_ID, _.Dest_ID}) |> @map({Source_IATA=key(_)[1], Dest_IATA=key(_)[2],Source_ID=key(_)[3], Dest_ID=key(_)[4],  
                  count=1}))

# create graph
g7, nodes7 = generategraph(flights_airports, "Source_IATA", "Dest_IATA", "undirected")

nodes_int = [x for x in 1:size(nodes7)[1]]
nodes_IATA_dict = Dict(zip(nodes7, nodes_int))

nodes_ids_dict = Dict(zip(nodes_int, nodes7))

src_selected = dropdown(nodes1, show_value=true, label = "Choose Source Airport", value = "JFK")
dst_selected = dropdown(nodes1, show_value=true, label = "Choose Destination Airport", value = "MSP")

@manipulate for src = src_selected, dst = dst_selected
    shortest_path = (enumerate_paths(dijkstra_shortest_paths(g7, nodes_IATA_dict[src]), nodes_IATA_dict[dst]))
    println("Shortest path between ",src_selected[], " and ", dst_selected[])
    for i in shortest_path
        print(nodes_ids_dict[i],"\t")
    end
    println("")
end
