package assignment2.task2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class AltListingsMain {

    private static Dataset<Row> getListingDs(SparkSession sparkSession) {
        return sparkSession
                .read()
                .option("delimiter", "\t")
                .option("header", true)
                .option("inferschema", true)
                .csv("airbnb_datasets/listings_us.csv");
    }

    private static Dataset<Row> getCalendarDs(SparkSession sparkSession) {
        return sparkSession
                .read()
                .option("delimiter", "\t")
                .option("header", true)
                .option("inferschema", true)
                .csv("airbnb_datasets/calendar_us.csv");

    }

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("Airbnb Alternative Listings")
                .master("local[*]")
                .getOrCreate();

        String listingId = args[0];

        DateFormat formatter = new SimpleDateFormat("YYYY-MM-DD");
        try {
            Date date = formatter.parse(args[1]);
        } catch (ParseException e) {
            System.out.println("Not a valid date. Format: YYYY-MM-DD");
        }

        int percentageHigher = Integer.parseInt(args[2]);
        float kmAway = Float.parseFloat(args[3]);
        int topN = Integer.parseInt(args[4]);

        Dataset<Row> listingsDs = getListingDs(sparkSession);
        Dataset<Row> calendarDs = getCalendarDs(sparkSession);


    }
}
