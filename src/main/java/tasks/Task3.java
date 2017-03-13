package tasks;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import scala.Tuple2;
import scala.Tuple3;

public class Task3 {

    /* Task 1.3 - 3 a) */
    public static void avgCityPrice(Dataset<Row> listingsDs) {
        listingsDs.select("city", "price")
                .map(row -> {
                    String priceString = row.getString(1);
                    float price = Float.valueOf(priceString
                            .replace("$", "")
                            .replace(",", ""));
                    return new Tuple2<>(row.getString(0), price);
                }, Encoders.tuple(Encoders.STRING(), Encoders.FLOAT()))
                .toDF("city", "price")
                .groupBy("city")
                .agg(functions.avg("price").as("avgprice"))
                .orderBy(functions.col("avgprice").desc())
                .coalesce(1)
                .write()
                .csv("./output/avgCityPrice");
    }

    /* Task 1.3 - 3 b) */
    public static void avgRoomTypePrice(Dataset<Row> listingsDs) {
        listingsDs.select("city", "room_type", "price")
                .map(row -> new Tuple3<String, String, Float>(row.getAs("city"), row.getAs("room_type"), Float.valueOf(
                        ((String) row.getAs("price"))
                                .replace("$", "")
                                .replace(",", "")))
                        , Encoders.tuple(Encoders.STRING(), Encoders.STRING(), Encoders.FLOAT()))
                .toDF("city", "room_type", "price")
                .groupBy("city", "room_type")
                .agg(functions.avg("price"))
                .orderBy(functions.col("city").asc())
                .repartition(1)
                .write().csv("./output/avgRoomTypePrice");
    }

    /* Task 1.3 - 3 c) */
    public static void avgNumReviewPerMonth(Dataset<Row> reviewsDs, Dataset<Row> listingsDs) {
        Dataset<Row> joinedSet = reviewsDs
                .select("listing_id", "id", "date")
                .join(listingsDs.select("city", "id"),
                        reviewsDs.col("listing_id").equalTo(listingsDs.col("id").as("listing_id")))
                .toDF("listing_id", "id", "date", "city", "listing_id_2");

        joinedSet
                .select("id", "date", "city")
                .map(row -> {
                    String month = row.getDate(1)
                            .toString()
                            .substring(0, 7);
                    return new Tuple3<>(row.getInt(0), month, row.getString(2));
                }, Encoders.tuple(Encoders.INT(), Encoders.STRING(), Encoders.STRING()))
                .toDF("id", "date", "city")
                .groupBy("date", "city")
                .count()
                .orderBy(functions.col("count").desc())
                .coalesce(1)
                .write()
                .csv("./output/avgNumReviewPerMonth");
    }

    /* Task 1.3 - 3 d) */
    public static void estimatedBookingsPerYear(Dataset<Row> listingsDs) {
        double percentageWhoLeavesReview = 0.7;
        int avgNoOfNights = 3;

        listingsDs
                .select("reviews_per_month", "city")
                .groupBy("city")
                .agg(functions.round(functions.sum(functions.col("reviews_per_month")
                        .multiply(avgNoOfNights * 12)
                        .divide(percentageWhoLeavesReview)
                ))
                        .alias("nightsPerYear"))
                .orderBy(functions.col("nightsPerYear").desc())
                .coalesce(1)
                .write()
                .csv("./output/estimatedBookingsPerYear");

    }

    /* Task 1.3 - 3 e)*/

    public static void moneySpentPerYear(Dataset<Row> listingsDs) {
        double percentageWhoLeavesReview = 0.7;
        int avgNoOfNights = 3;

        listingsDs
                .select("city", "reviews_per_month", "price")
                .map(row -> new Tuple3<>(
                        row.getAs("city"), row.getAs("reviews_per_month"),
                        Float.valueOf(
                                ((String) row.getAs("price"))
                                        .replace("$", "")
                                        .replace(",", ""))
                ), Encoders.tuple(Encoders.STRING(), Encoders.DOUBLE(), Encoders.FLOAT()))
                .toDF("city", "reviews_per_month", "price")
                .groupBy("city")
                .agg(functions.sum(functions.col("reviews_per_month")
                        .multiply(functions.col("price"))
                        .multiply(avgNoOfNights * 12)
                        .divide(percentageWhoLeavesReview)).as("moneySpent"))
                .orderBy(functions.col("moneySpent").desc())
                .coalesce(1)
                .write()
                .csv("./output/moneySpentPerYear");
    }
}
