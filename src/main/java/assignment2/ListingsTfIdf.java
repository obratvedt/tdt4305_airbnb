package assignment2;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

public class ListingsTfIdf {

    public static void calculateListingsTfIdf(Dataset<Row> listingsDs, Dataset<Row> neighbourhoodsListingsDs, String listingId) {
        Dataset<String> words = listingsDs
                .select("description")
                .filter(functions.col("id").equalTo(listingId))
                .map(descRow -> {
                    String description = (String) descRow.getAs("description");
                    String descriptionCleaned = description.replaceAll("[^a-zA-Z ]", "").toLowerCase();
                    return descriptionCleaned;
                }, Encoders.STRING());

        Dataset<String> listingDescriptions = listingsDs
                .select("description")
                .map(descRow -> {
                    String description = (String) descRow.getAs("description");
                    String descriptionCleaned = description.replaceAll("[^a-zA-Z ]", "").toLowerCase();
                    return descriptionCleaned;
                }, Encoders.STRING());

        

    }
}
