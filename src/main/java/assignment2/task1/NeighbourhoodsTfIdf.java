package assignment2.task1;


import org.apache.spark.sql.*;

import java.util.Arrays;

public class NeighbourhoodsTfIdf {
    public static void calculateNeighbourhoodsTfIdf(Dataset<Row> listingsDs, Dataset<Row> neighbourhoodsListingsDs, String neighbourhood) {

        /* Second parameter for IdfFinder: concatinated descriptions for a neighbourhood */
        Dataset<String> neighbourhoodWords = listingsDs
                .select("id", "description")
                .join(neighbourhoodsListingsDs)
                .where(listingsDs.col("id").equalTo(neighbourhoodsListingsDs.col("id")))
                .toDF("id", "description", "listing_id", "neighbourhood")
                .where("neighbourhood = '" + neighbourhood + "'")
                .flatMap( row -> {
                    String cleanedDecsription = row.getAs("description");
                    return Arrays.asList(cleanedDecsription
                            .toLowerCase()
                            .replaceAll("[^a-z ]", "")
                            .split(" ")).iterator();
                }, Encoders.STRING());

        neighbourhoodWords.show();

        /* First idf parameter: Clean listing descriptions */
        Dataset<String> listingDescriptions = listingsDs
                .select("description")
                .filter(functions.col("description").isNotNull())
                .map(descRow -> {
                    String description = descRow.getAs("description");
                    return description.replaceAll("[^a-zA-Z ]", "").toLowerCase();
                }, Encoders.STRING());

        listingDescriptions.show();

        Dataset<Row> idf = IdfFinder.inverseDocumentFrequency(listingDescriptions, neighbourhoodWords.dropDuplicates());

        idf.show();

        neighbourhoodWords
                .withColumnRenamed("value", "word")
                .join(idf)
                .where(neighbourhoodWords.col("word").equalTo(idf.col("word")))
                .groupBy(functions.col("word"))
                .agg(functions.count("word").as("count"));

    }
}
