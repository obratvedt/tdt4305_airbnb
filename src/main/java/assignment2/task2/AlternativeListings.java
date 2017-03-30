package assignment2.task2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import javax.xml.crypto.Data;

/**
 * Created by sigurd on 3/30/17.
 */
public class AlternativeListings {

    static void find(Dataset<Row> listings, Dataset<Row> calendar, String date){

        Row chosenListing;
        String roomType = "";

        listings
                .select("id", "amenities", "room_type", "longitude", "latitude")
                .filter(functions.col("room_type").equalTo(roomType))
                .show();

        calendar
                .filter(functions.col("available").equalTo("t"))
                .filter(functions.col("date").equalTo(date));



    }

}
