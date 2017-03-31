package assignment2.task1;


import org.apache.spark.sql.*;
import scala.Tuple2;

import java.util.Arrays;

public class ListingsTfIdf {

    public static void calculateListingsTfIdf(Dataset<Row> listingsDs, String listingId) {
        //cleans the description of the listing that is to be calculated TF IDF on
        Dataset<String> words = listingsDs
                .select("description")
                .filter(functions.col("id").equalTo(listingId))
                .filter(functions.col("description").isNotNull())
                .flatMap(descRow -> {
                    String description = descRow.getAs("description");
                    String cleanDescription = description
                            .toLowerCase()
                            .replace(",", " ")
                            .replaceAll("[^-a-z ]", "")
                            .replaceAll(" +", " ")
                            .replaceAll("-+", "-")
                            .replaceAll("^-", "")
                            .replaceAll("-$", "");
                    return Arrays.asList(cleanDescription.split(" ")).iterator();
                }, Encoders.STRING());


        //cleans the descriptions of the listings that is used to calculate IDF
        Dataset<String> listingDescriptions = listingsDs
                .select("description")
                .filter(functions.col("description").isNotNull())
                .map(descRow -> {
                    String description = descRow.getAs("description");
                    return description
                            .toLowerCase()
                            .replace(",", " ")
                            .replaceAll("[^-a-z ]", "")
                            .replaceAll(" +", " ")
                            .replaceAll("-+", "-")
                            .replaceAll("^-", "")
                            .replaceAll("-$", "");
                }, Encoders.STRING());

        //Calculates the idfs
        Dataset<Row> idfs = IdfFinder.inverseDocumentFrequency(listingDescriptions, words.collectAsList());

        //Counts the words of the description
        long listingWordCount = words
                .flatMap(word -> Arrays.asList(word.split(" ")).iterator(), Encoders.STRING())
                .count();

        //Joins the words from the description with the idf dataset and calculates the TF and the TF-IDF.
        words
                .groupBy(functions.col("value"))
                .agg(functions.count("value").as("tfCount"))
                .join(idfs)
                .where("value = word")
                .map(row -> {
                    String word = row.getAs("value");
                    long count = row.getAs("tfCount");
                    double tf = (double) count / (double) listingWordCount;
                    float idf = row.getAs("idf");
                    double tfidf = tf * (double) idf;

                    return new Tuple2<>(word, tfidf);
                }, Encoders.tuple(Encoders.STRING(), Encoders.DOUBLE()))
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
