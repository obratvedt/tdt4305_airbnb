package tasks;


import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import scala.Tuple3;
import scala.Tuple4;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.row_number;

public class Task4 {
    /* Task 1.3 - 4 a)*/
    public static void avgListingsPerHost(Dataset<Row> listingsDs) {
        listingsDs
                .select("host_id")
                .groupBy("host_id")
                .count()
                .agg(functions.avg("count")
                        .as("averageNoOfListingsPerHost"))
                .show();
    }

    /* Task 1.3 - 4 b) */
    public static long percentHostMoreMultipleListings(Dataset<Row> listingsDs) {
        long all = listingsDs.count();
        long numHostMultipleListings = listingsDs.select("host_id", "id")
                .groupBy("host_id")
                .count()
                .filter(functions.col("count").gt(1))
                .count();
        System.out.println("All:" + all);
        System.out.println("numWithMutiple:" + numHostMultipleListings);

        /* 11.5% of all hosts have multiple listings */
        return (numHostMultipleListings / all);
    }

    /*  Task 1.3 - 4 c) */
    public static void top3highestIncomeCities(Dataset<Row> listings, Dataset<Row> calendar) {
        Dataset<Row> availableCalendar = calendar
                .filter(functions.col("available").equalTo("f"));

        Dataset<Row> sumPrice = listings.select("id", "city", "price", "host_id")
                .map(row ->
                                new Tuple4<Integer, String, Float, Integer>(
                                        row.getAs("id"),
                                        row.getAs("city"), Float.valueOf(
                                        ((String) row.getAs("price"))
                                                .replace("$", "")
                                                .replace(",", "")),
                                        row.getAs("host_id")),
                        Encoders.tuple(Encoders.INT(), Encoders.STRING(), Encoders.FLOAT(), Encoders.INT()))
                .toDF("id", "city", "price", "host_id")
                .join(availableCalendar)
                .where("id = listing_id")
                .groupBy("city", "host_id")
                .agg(functions.sum("price").as("sum_price"))
                .orderBy("sum_price");

        WindowSpec w = Window.partitionBy(sumPrice.col("city"))
                .orderBy(sumPrice.col("sum_price").desc());

        sumPrice
                .withColumn("rank", row_number().over(w))
                .where(col("rank").$less$eq(3))
                .drop("rank")
                .coalesce(1)
                .write()
                .mode("overwrite")
                .csv("./output/top3highestincomecities");
    }
}

