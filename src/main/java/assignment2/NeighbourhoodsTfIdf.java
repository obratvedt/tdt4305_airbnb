package assignment2;


import org.apache.spark.sql.*;
import scala.Tuple3;

import java.util.Arrays;

import static org.apache.hadoop.yarn.util.StringHelper.join;

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
                            .replaceAll("[^-a-z ]", "")
                            .split(" ")).iterator();
                }, Encoders.STRING());
        long wordsInNeighourhood = neighbourhoodWords.count();

        neighbourhoodWords.show();

        /* First idf parameter: Clean listing descriptions */
        Dataset<String> listingDescriptions = listingsDs
                .select("description")
                .filter(functions.col("description").isNotNull())
                .map(descRow -> {
                    String description = descRow.getAs("description");
                    return description.toLowerCase().replaceAll("[^-a-z ]", "");
                }, Encoders.STRING());

        listingDescriptions.show();

        Dataset<Row> idfs = IdfFinder.inverseDocumentFrequency(listingDescriptions, neighbourhoodWords.dropDuplicates());

        idfs.show();

        neighbourhoodWords
                .withColumnRenamed("value", "word")
                .groupBy(functions.col("word"))
                .agg(functions.count("word").as("count"))
                .join(idfs)
                .where(neighbourhoodWords.col("word").equalTo(idfs.col("word")))
                .map( row -> {
                    String word = row.getAs("word");
                    long count = row.getAs("count");
                    double tf = (double) count / (double) wordsInNeighourhood;
                    float idf = row.getAs("idf");
                    double tfidf = tf * (double) idf;

                    return new Tuple3<>(word, count, tfidf);
                }, Encoders.tuple(Encoders.STRING(), Encoders.LONG(), Encoders.DOUBLE()) )
                .show();

    }
}
