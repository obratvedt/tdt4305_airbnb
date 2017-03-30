package assignment2.task1;


import org.apache.spark.sql.*;
import scala.Tuple2;

import java.util.Arrays;

public class ListingsTfIdf {

    public static void calculateListingsTfIdf(Dataset<Row> listingsDs, Dataset<Row> neighbourhoodsListingsDs, String listingId) {
        Dataset<String> words = listingsDs
                .select("description")
                .filter(functions.col("id").equalTo(listingId))
                .flatMap( descRow -> {
                    String description =  descRow.getAs("description");
                    String cleanDescription = description.toLowerCase().replaceAll("[^-a-z ]", "");
                    return Arrays.asList(cleanDescription.split(" ")).iterator();
                }, Encoders.STRING());

        long listingWordCount = words
                .flatMap( word -> Arrays.asList(word.split(" ")).iterator(), Encoders.STRING() )
                .count();

        Dataset<String> listingDescriptions = listingsDs
                .select("description")
                .filter(functions.col("description").isNotNull())
                .map(descRow -> {
                    String description = descRow.getAs("description");
                    return description.toLowerCase().replaceAll("[^-a-z ]", "");
                }, Encoders.STRING());

        Dataset<Row> idfs = IdfFinder.inverseDocumentFrequency(listingDescriptions, words.collectAsList());

        words
                .groupBy(functions.col("value"))
                .agg(functions.count("value").as("tf"))
                .join(idfs)
                .where("value = word")
                .map( row -> {
                    String word = row.getAs("value");
                    long count = row.getAs("tf");
                    double tf = (double) count / (double) listingWordCount;
                    float idf = row.getAs("idf");
                    double tfidf = tf * (double) idf;

                    return new Tuple2<>(word, tfidf);
                }, Encoders.tuple(Encoders.STRING(), Encoders.DOUBLE()) )
                .toDF("word", "tfidf")
                .orderBy(functions.col("tfidf").desc())
                .limit(100)
                .coalesce(1)
                .write()
                .mode("overwrite")
                .option("header", true)
                .option("delimiter", "\t")
                .csv("output/listingIdf");

    }
}
