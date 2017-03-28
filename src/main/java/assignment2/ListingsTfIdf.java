package assignment2;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

public class ListingsTfIdf {

    public static void calculateListingsTfIdf(Dataset<Row> listingsDs, Dataset<Row> neighbourhoodsListingsDs, String listingId) {
        String words = listingsDs
                .select("description")
                .filter(functions.col("id").equalTo(listingId))
                .map(descRow -> {
                    String description =  descRow.getAs("description");
                    return description.replaceAll("[^a-zA-Z ]", "").toLowerCase();
                }, Encoders.STRING())
                .first();



        Dataset<String> listingDescriptions = listingsDs
                .select("description")
                .filter(functions.col("description").isNotNull())
                .map(descRow -> {
                    String description = descRow.getAs("description");
                    return description.replaceAll("[^a-zA-Z ]", "").toLowerCase();
                }, Encoders.STRING());

        IdfFinder.inverseDocumentFrequency(listingDescriptions, words);

    }
}
