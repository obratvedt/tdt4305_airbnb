
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.scalatest.EncodedOrdering;
import scala.Double;
import scala.Tuple2;
import scala.Tuple3;
import schemas.ListingsSchema;
import schemas.ReviewSchema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SparkMain {

    private static Dataset<Row> getReviewDs(SparkSession sparkSession) {
        return sparkSession
                .read()
                .option("delimiter", "\t")
                .option("header", true)
                .schema(ReviewSchema.getReviewSchema())
                .csv("airbnb_datasets/reviews_us.csv");
    }

    private static Dataset<Row> getListingDs(SparkSession sparkSession) {
        return sparkSession
                .read()
                .option("delimiter", "\t")
                .option("header", true)
                .option("inferschema", true)
                //.schema(ListingsSchema.getListingsSchema())
                .csv("airbnb_datasets/listings_us.csv");
    }

    private static Dataset<Row> getCalendarDs(SparkSession sparkSession) {
        return sparkSession
                .read()
                .option("delimiter", "\t")
                .option("header", true)
                .option("inferschema", true)
                .csv("airbnb_datasets/calendar_us.csv");
    }

    //Not working yet, need a parser.
    private static Dataset<Row> getNeighbourhoodsDs(SparkSession sparkSession) {
        return sparkSession
                .read()
                .json("airbnb_datasets/neighbourhoods.geojson");
    }


    /* Task 1.3 - 2 b) */
    private static void distinctListings(Dataset<Row> listingsDs) {
        List<String> results = new ArrayList<>();
        Arrays.stream(listingsDs.schema().fieldNames())
                .forEach((fieldName) -> {
                    long count = listingsDs.select(fieldName).distinct().count();
                    results.add(fieldName + ":" + count);
                });
        results.forEach(result -> System.out.println(result));
    }


    /* Task 1.3 - 2 c) */
    private static void listCities(Dataset<Row> listingsDs) {
        List<String> cities = new ArrayList<>();
        listingsDs
                .select("city")
                .distinct()
                .collect();
    }

    /* Task 1.3 - 3 a) */
    private static void avgCityPrice(Dataset<Row> listingsDs) {
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
    private static void avgRoomTypePrice(Dataset<Row> listingsDs) {
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
                .write().csv("output");
    }

    /* Task 1.3 - 3 c) */
    private static void avgNumReviewPerMonth(Dataset<Row> reviewsDs, Dataset<Row> listingsDs) {
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
    private static void estimatedBookingsPerYear(Dataset<Row> listingsDs) {
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

    private static void moneySpentPerYear(Dataset<Row> listingsDs) {
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


    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("Airbnb analyis")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> reviewsDs = getReviewDs(sparkSession);
        Dataset<Row> listingDs = getListingDs(sparkSession);
        //moneySpentPerYear(listingDs);
        //avgNumReviewPerMonth(reviewsDs, listingDs);
        //estimatedBookingsPerYear(listingDs);
        //reviewsDs.show();
        //listingDs.show();
        /*
        avgCityPrice(listingDs);
        avgRoomTypePrice(listingDs);*/

        //distinctListings(listingDs);

    }

}

