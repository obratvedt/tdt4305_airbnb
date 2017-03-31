package assignment2.task1;


import org.apache.spark.sql.*;
import scala.Tuple2;

import java.util.Arrays;


public class NeighbourhoodsTfIdf {
    public static void calculateNeighbourhoodsTfIdf(Dataset<Row> listingsDs, Dataset<Row> neighbourhoodsListingsDs, String neighbourhood) {

        // Second parameter for IdfFinder: concatinated descriptions for a neighbourhood
        //Creates a dataset of all the words in the neighbourhood. Cleaned with regex
        Dataset<String> neighbourhoodWords = listingsDs
                .select("id", "description")
                .filter(functions.col("description").isNotNull())
                .join(neighbourhoodsListingsDs)
                .where(listingsDs.col("id").equalTo(neighbourhoodsListingsDs.col("id")))
                .toDF("id", "description", "listing_id", "neighbourhood")
                .where("neighbourhood = '" + neighbourhood + "'")
                .flatMap(row -> {
                    String decsription = row.getAs("description");
                    return Arrays.asList(decsription
                            .toLowerCase()
                            .replace(",", " ")
                            .replaceAll("[^-a-z ]", "")
                            .replaceAll(" +", " ")
                            .replaceAll("-+", "-")
                            .replaceAll("^-", "")
                            .replaceAll("-$", "")
                            .split(" ")).iterator();
                }, Encoders.STRING());


        // First idf parameter: Clean listing descriptions
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
        Dataset<Row> idfs = IdfFinder.inverseDocumentFrequency(listingDescriptions, neighbourhoodWords.dropDuplicates().collectAsList());

        //Counts the words of the neighbourhood descriptions
        long wordsInNeighourhood = neighbourhoodWords.count();

        //Joins the words from the neighbourhood descriptions with the idf dataset and calculates the TF and the TF-IDF.
        neighbourhoodWords
                .withColumnRenamed("value", "_word")
                .groupBy(functions.col("_word"))
                .agg(functions.count("_word").as("count"))
                .join(idfs)
                .where("_word = word")
                .map(row -> {
                    String word = row.getAs("_word");
                    long count = row.getAs("count");
                    double tf = (double) count / (double) wordsInNeighourhood;
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
                .csv("output/neighbourhoodTfIdf");

    }
}
