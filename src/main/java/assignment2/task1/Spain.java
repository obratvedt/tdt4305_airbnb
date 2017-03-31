package assignment2.task1;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class Spain {

    private static Dataset<Row> getListingDs(SparkSession sparkSession, String pathToDs) {
        return sparkSession
                .read()
                .option("delimiter", "\t")
                .option("header", true)
                .option("inferschema", true)
                .csv(pathToDs + "listings_us.csv");
    }

    private static Dataset<Row> getNeighbourhoodListingsDs(SparkSession sparkSession, String pathToDs) {
        return sparkSession
                .read()
                .option("delimiter", "\t")
                .option("header", true)
                .option("inferschema", true)
                .csv(pathToDs + "listings_ids_with_neighborhoods.tsv");
    }

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("Airbnb TF-IDF")
                .master("local[*]")
                .getOrCreate();
        String pathToDs = args[0];

        Dataset<Row> neighbourhoodsListingsDs = getNeighbourhoodListingsDs(sparkSession, pathToDs);
        Dataset<Row> listingsDs = getListingDs(sparkSession, pathToDs);

        if (args[1].equals("-l")) {
            String listingId = args[2];
            ListingsTfIdf.calculateListingsTfIdf(listingsDs, neighbourhoodsListingsDs, listingId);

        } else if (args[1].equals("-n")) {
            String neighbourhood = args[2];
            NeighbourhoodsTfIdf.calculateNeighbourhoodsTfIdf(listingsDs, neighbourhoodsListingsDs, neighbourhood);
        } else {
            System.out.println("Input should be:" +
                    "<full_path_to_datasets> -l <listing_id> \n" +
                    "or: <full_path_to_datasets> -n <neighborhood name>");
        }
    }

}
