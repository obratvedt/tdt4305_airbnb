package assignment2.task1;


import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

import java.util.*;


public class IdfFinder {

    /**
     * WARINING: Only feed me with lowercase and no punctuations
     *
     * @param descriptions, listings with ID, and descriptions, cleaned beforehand
     * @param words         words which frequencies need to be calculated
     * @return inverseDocumentFrequency
     */
    public static Dataset<Row> inverseDocumentFrequency(Dataset<String> descriptions, List<String> words) {
        long documentCount = descriptions.count();

        //Returns a dataset with (word, idf) based on the terms from the description to calculate TF-IDF on
        return descriptions
                .map(s -> new HashSet<String>(Arrays.asList(s.split(" "))).toString()
                                .replace(",", "")
                                .replace("]", "")
                                .replace("[", "")
                        , Encoders.STRING())
                .flatMap(s -> Arrays.asList(s.split(" ")).iterator(), Encoders.STRING())
                .filter(s -> words.contains(s))
                .groupBy(functions.col("value"))
                .agg(functions.count("value").as("count"))
                .map(row -> {

                            String word = row.getAs("value");
                            float idf = (float) documentCount / ((long) row.getAs("count"));
                            return RowFactory.create(word, idf);
                        },
                        RowEncoder.apply(
                                DataTypes.createStructType(new StructField[]{
                                        DataTypes.createStructField("word", DataTypes.StringType, true),
                                        DataTypes.createStructField("idf", DataTypes.FloatType, true),
                                })
                        )
                );
    }

}
