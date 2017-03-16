package tasks;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import scala.Tuple3;

import static org.apache.spark.sql.functions.*;

public class Task5 {

    /* Task 1.3 5 a)*/
    public static void topGuests(Dataset<Row> listingsDs, Dataset<Row> reviewsDs) {
        Dataset<Row> joinedSet = reviewsDs
                .select("reviewer_id", "listing_id", "reviewer_name")
                .join(listingsDs.select("id", "city")
                        , listingsDs.col("id").equalTo(reviewsDs.col("listing_id")));

        Dataset<Row> bookingCount = joinedSet.select("reviewer_id", "city")
                .groupBy("city", "reviewer_id")
                .count();

        WindowSpec w = Window.partitionBy(bookingCount.col("city"))
                .orderBy(bookingCount.col("count").desc());

        bookingCount
                .withColumn("rank", row_number().over(w))
                .where(col("rank").$less$eq(3))
                .drop("rank")
                .coalesce(1)
                .write()
                .mode("overwrite")
                .csv("./output/topGuests");
    }

    /* Task 1.3 5 b)*/
    public static void richestGuest(Dataset<Row> reviewsDs, Dataset<Row> listingsDs){
        Dataset<Row> joinedSet = reviewsDs
                .select("reviewer_id", "listing_id", "reviewer_name")
                .join(listingsDs.select("id", "price")
                        , listingsDs.col("id").equalTo(reviewsDs.col("listing_id")));
        joinedSet
                .select("reviewer_id", "reviewer_name", "price")
                .map(row -> new Tuple3<String, String, Float>(
                        row.getAs("reviewer_id"), row.getAs("reviewer_name"),
                        Float.valueOf(((String) row.getAs("price"))
                                .replace("$", "")
                                .replace(",", "")))
                , Encoders.tuple(Encoders.STRING(), Encoders.STRING(), Encoders.FLOAT()))
                .toDF("reviewer_id", "reviewer_name", "price")
                .groupBy("reviewer_id", "reviewer_name")
                .agg(sum(col("price")).as("total_price"))
                .orderBy(col("total_price").desc())
                .limit(1)
                .show();
    }
}
