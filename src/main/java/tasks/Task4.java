package tasks;


import org.apache.spark.sql.*;
import scala.Tuple3;
import scala.Tuple4;

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
    public static long percentHostMoreMultipleListings(Dataset<Row> listingsDs){
        long all = listingsDs.count();
        long numHostMultipleListings = listingsDs.select("host_id", "id")
                .groupBy("host_id")
                .count()
                .filter(functions.col("count").gt(1))
                .count();
        System.out.println("All:" + all);
        System.out.println("numWithMutiple:" + numHostMultipleListings);

        /* 11.5% of all hosts have multiple listings */
        return (numHostMultipleListings/all);
    }

    /*  */
    public static void top3highestIncomeCities(Dataset<Row> listings, Dataset<Row> calendar){
        Dataset<Row> availableCalendar = calendar
                .filter(functions.col("available").equalTo("f"));

        listings.select("id",  "city", "price", "host_id")
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
                .groupBy("city","host_id")
                .agg(functions.sum("price").as("sum_price"))
                .orderBy("sum_price")
                .show();
    }
}

