package assignment2.task2;

import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import scala.Tuple9;


import javax.validation.constraints.Null;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;


/**
 * Created by sigurd on 3/30/17.
 */
public class AlternativeListings {

    static void find(Dataset<Row> listings, Dataset<Row> calendar, String date, int listingId, int percentageHigher, float kmAway, int topN) {

        Row chosenListing = listings
                .select("id", "amenities", "longitude", "latitude", "room_type", "price")
                .filter(functions.col("id").equalTo(listingId))
                .first();

        double latitude = chosenListing.getAs("latitude");
        double longitude = chosenListing.getAs("longitude");
        String roomType = chosenListing.getAs("room_type");
        String amenities = chosenListing.getAs("amenities");
        Float price = parsePrice(chosenListing.getAs("price"));

        Dataset<Row> filteredListings = listings
                .select("id", "name", "amenities", "room_type", "longitude", "latitude", "price", "review_scores_rating", "picture_url")
                .filter(functions.col("room_type").equalTo(roomType))
                .map( row -> {
                    double thisLatitude = row.getAs("latitude");
                    double thisLongitude = row.getAs("longitude");
                    double distanceFromListing = Haversine.haversine(latitude, longitude, thisLatitude, thisLongitude);
                    String image = row.getAs("picture_url");

                    double rating = 0;
                    try{
                        rating = row.getAs("review_scores_rating");
                    }
                    catch (NullPointerException e){
                        rating = 0;
                    }
                    int noOfCommonAmenities = haveAmenities(amenities, row.getAs("amenities"));
                    float thisPrice = parsePrice(row.getAs("price"));

                    return RowFactory.create((int)row.getAs("id"), (String)row.getAs("name"), noOfCommonAmenities,
                            distanceFromListing, thisPrice, thisLatitude, thisLongitude, image, rating);

                }, RowEncoder.apply(
                        DataTypes.createStructType(
                                new StructField[] {
                                        DataTypes.createStructField("id", DataTypes.IntegerType, true),
                                        DataTypes.createStructField("name", DataTypes.StringType, true),
                                        DataTypes.createStructField("noOfCommonAmenities", DataTypes.IntegerType, true),
                                        DataTypes.createStructField("distance", DataTypes.DoubleType, true),
                                        DataTypes.createStructField("price", DataTypes.FloatType, true),
                                        DataTypes.createStructField("latitude", DataTypes.DoubleType, true),
                                        DataTypes.createStructField("londitude", DataTypes.DoubleType, true),
                                        DataTypes.createStructField("image", DataTypes.StringType, true),
                                        DataTypes.createStructField("rating", DataTypes.DoubleType, true),
                                }
                        )))
                .filter(functions.col("distance").$less$eq(kmAway))
                .filter(row -> {
                    Float givenPrice = row.getAs("price");
                    Float maxPrice = price * (1 + (percentageHigher / 100.0f));
                    return givenPrice <= maxPrice;
                })
                .filter(functions.col("id").notEqual(listingId));

        calendar
                .filter(functions.col("available").equalTo("t"))
                .filter(functions.col("date").equalTo(date))
                .join(filteredListings)
                .where("id = listing_id")
                .select("id", "name", "noOfCommonAmenities", "distance", "price")
                .orderBy(functions.col("noOfCommonAmenities").desc())
                .limit(topN)
                .coalesce(1)
                .write()
                .mode("overwrite")
                .option("header", true)
                .option("delimiter", "\t")
                .csv("output/alternativeListings");


    }

    private static Integer haveAmenities(String requiredAmenities, String amenities) {
        Set<String> amenitiesSet = getAmenities(amenities);
        amenitiesSet.retainAll(getAmenities(requiredAmenities));
        return amenitiesSet.size();
    }

    private static Set<String> getAmenities(String amenties) {
        String[] amenityArray = amenties.replace("{", "")
                .replace("}", "")
                .replace("\"", "")
                .split(",");
        Set<String> set = new HashSet<>();
        Collections.addAll(set, amenityArray);
        return set;
    }

    private static Float parsePrice(String price) {
        return Float.valueOf(price.replace("$", "").replace(",", ""));
    }
}
