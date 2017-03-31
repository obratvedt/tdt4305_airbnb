package assignment2.task2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


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

    /**
     * Main method for alternative listings
     *
     * @param args
     */

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("Airbnb Alternative Listings")
                .master("local[*]")
                .getOrCreate();

        int listingId = Integer.parseInt(args[0]);
        String date = args[1];

        int percentageHigher = Integer.parseInt(args[2]);
        float kmAway = Float.parseFloat(args[3]);
        int topN = Integer.parseInt(args[4]);

        Dataset<Row> listingsDs = getListingDs(sparkSession);
        Dataset<Row> calendarDs = getCalendarDs(sparkSession);

        AlternativeListings.find(listingsDs, calendarDs, date, listingId, percentageHigher, kmAway, topN);
    }
}
