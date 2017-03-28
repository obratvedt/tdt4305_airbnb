package assignment2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import tasks.Task6;

public class Spain {

    private static Dataset<Row> getListingDs(SparkSession sparkSession) {
        return sparkSession
                .read()
                .option("delimiter", "\t")
                .option("header", true)
                .option("inferschema", true)
                .csv("airbnb_datasets/listings_us.csv");
    }

    private static Dataset<Row> getNeighbourhoodListingsDs(SparkSession sparkSession) {
        return sparkSession
                .read()
                .option("delimiter", "\t")
                .option("header", true)
                .option("inferschema", true)
                .csv("airbnb_datasets/listings_ids_with_neighborhoods.tsv");
    }
    
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("Airbnb TF-IDF")
                .master("local[*]")
                .getOrCreate();

        if (args[1].equals("-l")) {
            //something
        } else if (args[1].equals("-n")) {
            //something
        } else {
            System.out.println("Input should be:" +
                    "<full_path_to_datasets> -l <listing_id> \n" +
                    "or: <full_path_to_datasets> -n <neighborhood name>");
        }
    }

}
