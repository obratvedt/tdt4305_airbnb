package tasks;

import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.geom.impl.CoordinateArraySequence;
import org.apache.spark.sql.*;
import org.geotools.geojson.geom.GeometryJSON;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import scala.Tuple2;
import scala.Tuple3;

import java.io.FileReader;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;


/**
 * Created by sigurd on 3/16/17.
 */

public class Task6 implements Serializable {

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


    private static List<Area> createAreas(JSONArray areas){
        return (List<Area>) areas.stream()
                .map((area) -> {
                    return new Area((JSONObject) area);
                }).collect(Collectors.toList());
    }


    /* Assign a neighborhood name to each listing */
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
                return new Tuple3<>( id, neighbourhood, city );
            }, Encoders.tuple(Encoders.INT() ,Encoders.STRING(), Encoders.STRING()) )
                .toDF("my_id", "my_neighbourhood", "my_city")
                .orderBy(functions.col("my_id").asc());
    }

    public static void percentMatchWithTest(Dataset<Row> listings, Dataset<Row> neighbourhoodTestSet){
        Dataset<Row> mappedListings = mapNeigbourhoodsToListings(listings);

        Dataset<Row> joinedSet = neighbourhoodTestSet
                .join(mappedListings, mappedListings
                        .col("my_id")
                        .equalTo(neighbourhoodTestSet.col("id"))
                        .as("id"));
        long count = joinedSet.count();
        long filteredCount = joinedSet
                .filter( functions.col("neighbourhood").equalTo("my_neighbourhood") )
                .count();

        System.out.println(String.format("All: %l, filtered: %l, percent: %l", count, filteredCount, filteredCount/count));

    }

    private static String findNeighbourhood(double lon, double lat, List<Area> areas){
        for(Area area : areas){
            if(inside(lon, lat, area)){
                return area.getNeighbourhood();
            }
        }
        return null;
    }


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
