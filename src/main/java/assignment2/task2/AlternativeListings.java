package assignment2.task2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import scala.Tuple5;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;


public class AlternativeListings {

    static void find(Dataset<Row> listings, Dataset<Row> calendar, String date, int listingId, int percentageHigher, float kmAway, int topN) {
        //Extracts the listing to find alternative listings on
        Row chosenListing = listings
                .select("id", "amenities", "longitude", "latitude", "room_type", "price")
                .filter(functions.col("id").equalTo(listingId))
                .first();

        //Extract the values from the row above
        double latitude = chosenListing.getAs("latitude");
        double longitude = chosenListing.getAs("longitude");
        String roomType = chosenListing.getAs("room_type");
        String amenities = chosenListing.getAs("amenities");
        Float price = parsePrice(chosenListing.getAs("price"));

        //Filters the listings on the room type, distance, common amenities, price.
        Dataset<Row> filteredListings = listings
                .select("id", "name", "amenities", "room_type", "longitude", "latitude", "price")
                .filter(functions.col("room_type").equalTo(roomType))
                .map(row -> {
                    double thisLatitude = row.getAs("latitude");
                    double thisLongitude = row.getAs("longitude");
                    double distanceFromListing = Haversine.haversine(latitude, longitude, thisLatitude, thisLongitude);
                    int noOfCommonAmenities = haveAmenities(amenities, row.getAs("amenities"));
                    float thisPrice = parsePrice(row.getAs("price"));

                    return new Tuple5<>(row.getAs("id"), row.getAs("name"), noOfCommonAmenities, distanceFromListing, thisPrice);

                }, Encoders.tuple(Encoders.INT(), Encoders.STRING(), Encoders.INT(), Encoders.DOUBLE(), Encoders.FLOAT()))
                .toDF("id", "name", "noOfCommonAmenities", "distance", "price")
                .filter(functions.col("distance").$less$eq(kmAway))
                .filter(row -> {
                    Float givenPrice = row.getAs("price");
                    Float maxPrice = price * (1 + (percentageHigher / 100.0f));
                    return givenPrice <= maxPrice;
                })
                .filter(functions.col("id").notEqual(listingId));

        //removes calendar rows that are not available and not the correct date
        //joins it with the filtered listings dataset and ordered by the number of common amenities
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

    /**
     * Helper method to calculate the number of common amenities
     *
     * @param requiredAmenities
     * @param amenities
     * @return
     */
    private static Integer haveAmenities(String requiredAmenities, String amenities) {
        Set<String> amenitiesSet = getAmenities(amenities);
        amenitiesSet.retainAll(getAmenities(requiredAmenities));
        return amenitiesSet.size();
    }

    /**
     * Creates a set of the amenities
     *
     * @param amenities
     * @return
     */
    private static Set<String> getAmenities(String amenities) {
        String[] amenityArray = amenities.replace("{", "")
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
