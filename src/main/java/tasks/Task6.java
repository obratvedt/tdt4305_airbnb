package tasks;

import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.geom.impl.CoordinateArraySequence;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by sigurd on 3/16/17.
 */

public class Task6 {

    public static JSONObject getData(){
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

    public static List<Area> createAreas(JSONArray areas){
        return (List<Area>) areas.stream()
                .map((area) -> {
                    return new Area((JSONObject) area);
                }).collect(Collectors.toList());

    }

    /* Assign a neighborhood name to each listing */
    public static void run(Dataset<Row> listings){
        JSONObject data = getData();
        System.out.println(data);
        JSONArray areasJson = (JSONArray) data.get("features");
        List<Area> areas = createAreas(areasJson);
        List coordinates = areas.get(0).getCoordinates();
        coordinates.stream().forEach( (coordinate) -> {
            System.out.println(coordinate);
        });
        GeometryFactory.
        Polygon polygon = new Polygon()


    }

    public boolean inside(){
        final GeometryFactory gf = new GeometryFactory();

        final List<Coordinate> points = new ArrayList<Coordinate>();
        points.add(new Coordinate(-10, -10));
        points.add(new Coordinate(-10, 10));
        points.add(new Coordinate(10, 10));
        points.add(new Coordinate(10, -10));
        points.add(new Coordinate(-10, -10));
        final Polygon polygon = gf.createPolygon(new LinearRing(new CoordinateArraySequence(points
                .toArray(new Coordinate[points.size()])), gf), null);

        final Coordinate coord = new Coordinate(0, 0);
        final Point point = gf.createPoint(coord);

        System.out.println(point.within(polygon));
    }

}
