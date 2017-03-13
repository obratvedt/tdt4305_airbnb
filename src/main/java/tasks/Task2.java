package tasks;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Task2 {
    /* Task 1.3 - 2 b) */
    public static void distinctListings(Dataset<Row> listingsDs) {
        List<String> results = new ArrayList<>();
        Arrays.stream(listingsDs.schema().fieldNames())
                .forEach((fieldName) -> {
                    long count = listingsDs.select(fieldName).distinct().count();
                    results.add(fieldName + ":" + count);
                });
        results.forEach(result -> System.out.println(result));
    }

    /* Task 1.3 - 2 c) */
    public static void distinctHosts(Dataset<Row> listingsDs) {
        long noOfHosts = listingsDs
                .select("host_id")
                .distinct()
                .count();

        System.out.println("Number of distinct hosts: " + noOfHosts);

    }

    /* Task 1.3 - 2 c) */
    public static void listCities(Dataset<Row> listingsDs) {
        List<Row> cities = new ArrayList<>();
        cities = listingsDs
                .select("city")
                .distinct()
                .collectAsList();

        for (Row city : cities){
            System.out.println(city.get(0));
        }
    }

}
