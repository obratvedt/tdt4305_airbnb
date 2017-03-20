package tasks;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import static org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.Scope.col;

public class Task131 {

    public static void getMapViz(Dataset<Row> listingsDs) {
        listingsDs
                .select(functions.col("host_name"), functions.col("beds"), functions.col("latitude").as("lat"), functions.col("longitude").as("lon"))
                .coalesce(1)
                .write()
                .mode("overwrite")
                .option("header", true)
                .csv("./output/cartoMapViz");
    }
}
