package tasks;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

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
}

