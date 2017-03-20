package tasks;

import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.geom.impl.CoordinateArraySequence;
import org.apache.spark.sql.*;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import scala.Tuple2;
import scala.Tuple4;

import java.io.FileReader;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;


/**
 * Created by sigurd on 3/16/17.
 */

public class Task6 implements Serializable {

    /**
     * Read geojson file to JSONObject
     * @return
     */
    private static JSONObject readData(){
        JSONParser parser = new JSONParser();
        JSONObject data = null;
        try{
            data = (JSONObject) parser.parse(new FileReader("./airbnb_datasets/neighbourhoods.geojson"));
        }
        catch (Exception e){
            System.out.println(e);
        }

        return data;
    }

    /**
     * Create list of wrapped JSONObjec containing neighbourhood data using the Area class
     * @param areas
     * @return
     */
    private static List<Area> createAreas(JSONArray areas){
        return (List<Area>) areas.stream()
                .map((area) -> {
                    return new Area((JSONObject) area);
                }).collect(Collectors.toList());
    }

    /**
     * Assign a neighborhood name to each listing and only include fields that are used later in these exercises
     * @param listings
     * @return
     */
    public static Dataset<Row> mapNeigbourhoodsToListings(Dataset<Row> listings){
        JSONObject data = readData();
        JSONArray areasJson = (JSONArray) data.get("features");
        final List<Area> areas = createAreas(areasJson);

        return listings
            .map( listing -> {
                int id = listing.getAs("id");
                double lat = listing.getAs("latitude");
                double lon = listing.getAs("longitude");
                String city = listing.getAs("city");
                String neighbourhood = findNeighbourhood( lon, lat, areas );
                String amenities = listing.getAs("amenities");
                return new Tuple4<>( id, neighbourhood, city, amenities );
            }, Encoders.tuple(Encoders.INT() ,Encoders.STRING(), Encoders.STRING(), Encoders.STRING()) )
                .toDF("my_id", "my_neighbourhood", "my_city", "amenities")
                .orderBy(functions.col("my_id").asc());
    }

    /**
     * Print to console how many percent of the given neighbourhood test dataset that matches with our neighbourhood assignments
     * @param listings
     * @param neighbourhoodTestSet
     */
    public static void percentMatchWithTest(Dataset<Row> listings, Dataset<Row> neighbourhoodTestSet){
        Dataset<Row> mappedListings = mapNeigbourhoodsToListings(listings);

        Dataset<Row> joinedSet = neighbourhoodTestSet
                .join(mappedListings, mappedListings
                        .col("my_id")
                        .equalTo(neighbourhoodTestSet.col("id"))
                        .as("id"));

        long count = joinedSet.count();
        long filteredCount = joinedSet
                .filter( functions.col("neighbourhood").equalTo(functions.col("my_neighbourhood")) )
                .count();

        System.out.println(String.format("All: %d, filtered: %d, percent: %d", count, filteredCount, filteredCount/count));
    }

    /**
     * Write to file a list of all distinct amenities for each neighbourhood
     * @param listings
     */
    public static void distinctAmenitiesPerNeighbourhood(Dataset<Row> listings){
        Dataset<Row> mappedListings = mapNeigbourhoodsToListings(listings);

        mappedListings
                .map( row -> {
                    String amenities = ((String)row.getAs("amenities"))
                            .replace("{", "")
                            .replace("}", "")
                            .replace("\"", "");
                    String neighbourhood = row.getAs("my_neighbourhood");
                    return new Tuple2<>( neighbourhood, amenities );
                }, Encoders.tuple(Encoders.STRING(), Encoders.STRING()) )
                .toJavaRDD()
                .mapToPair( stringStringTuple2 -> new Tuple2<>( stringStringTuple2._1, stringStringTuple2._2 ) )
                .reduceByKey( (s1, s2) -> {
                    HashSet set = new HashSet<String>( Arrays.asList( s1.split(",") ) );
                    set.addAll( Arrays.asList( s2.split(",") ) );
                    return String.join(",", set);
                })
                .coalesce(1)
                .saveAsTextFile("./output/6b");

    }

    /**
     * Get name of neighbourhood that given longitude and latitude is inside
     * @param lon
     * @param lat
     * @param areas
     * @return The first area in the list of areas that the coordinates reside inside
     */
    private static String findNeighbourhood(double lon, double lat, List<Area> areas){
        for(Area area : areas){
            if(inside(lon, lat, area)){
                return area.getNeighbourhood();
            }
        }
        return null;
    }

    /**
     * Check if a point is inside a given area using the JTS-geometry lib
     * @param lon longitude of point to check
     * @param lat latitude of point to check
     * @param area area that is to be checked against
     * @return
     */
    private static boolean inside(double lon, double lat ,Area area){
        final GeometryFactory gf = new GeometryFactory();
        final List<Coordinate> points = new ArrayList<>();

        for(JSONArray coord : area.getCoordinates()){
            double areaLon = (double) coord.get(0);
            double areaLat = (double) coord.get(1);
            points.add(new Coordinate(areaLon, areaLat));
        }

        final Polygon polygon = gf.createPolygon(new LinearRing(new CoordinateArraySequence(points
                .toArray(new Coordinate[points.size()])), gf), null);

        final Coordinate coord = new Coordinate(lon, lat);
        final Point point = gf.createPoint(coord);

        return point.within(polygon);
    }

}
