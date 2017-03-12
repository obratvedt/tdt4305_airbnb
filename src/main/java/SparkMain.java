
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.scalatest.EncodedOrdering;
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
                .forEach( (fieldName) -> {
                    long count = listingsDs.select( fieldName ).distinct().count();
                    results.add(fieldName + ":" + count);
                });
        results.forEach( result -> System.out.println(result));
    }


    /* Task 1.3 - 2 c) */
    private static void listCities(Dataset<Row> listingsDs){
        List<String> cities = new ArrayList<>();
        listingsDs
                .select("city")
                .distinct()
                .collect();
    }

    /* Task 1.3 - 3 a) */
    private static void avgCityPrice(Dataset<Row> listingsDs){
        listingsDs.select("city", "price")
                .map( row -> {
                    String priceString = row.getString(1);
                    float price = Float.valueOf(priceString
                            .replace("$","")
                            .replace(",",""));
                    return new Tuple2<>(row.getString(0) ,price);
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
    private static void avgRoomTypePrice(Dataset<Row> listingsDs){
        listingsDs.select("city", "room_type", "price")
                .map(row -> new Tuple3<String, String, Float>(row.getAs("city"), row.getAs("room_type"), Float.valueOf(
                        ((String) row.getAs("price"))
                                .replace("$", "")
                                .replace(",", "")))
                        , Encoders.tuple(Encoders.STRING(), Encoders.STRING(), Encoders.FLOAT()))
                .toDF("city", "room_type", "price")
                .groupBy( "city","room_type")
                .agg(functions.avg("price"))
                .orderBy(functions.col("city").asc())
                .repartition(1)
                .write().csv("output");
    }

    /* Task 1.3 - 3 c) */
    private static void avgNumReviewPerMonth(Dataset<Row> reviewsDs, Dataset<Row> listingsDs){
        Dataset<Row> joinedSet = reviewsDs
                .select("listing_id","id", "date")
                .join(listingsDs.select("city", "id"),
                reviewsDs.col("listing_id").equalTo(listingsDs.col("id").as("listing_id")))
                .toDF("listing_id", "id","date","city", "listing_id_2");

        joinedSet
                .select("id", "date", "city")
                .map( row -> {
                    String month = row.getDate(1)
                            .toString()
                            .substring(0,7);
                    return new Tuple3<>(row.getInt(0), month, row.getString(2));
                }, Encoders.tuple(Encoders.INT(), Encoders.STRING(), Encoders.STRING()))
                .toDF("id", "date", "city")
                .groupBy("date","city")
                .count()
                .orderBy(functions.col("count").desc())
                .coalesce(1)
                .write()
                .csv("./output/avgNumReviewPerMonth");
    }

    private void estimatedBookingsPerYear(Dataset<Row> reviewsDs){
        double percentageWhoLeavesReview = 0.7;



    }


    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("Airbnb analyis")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> reviewsDs = getReviewDs(sparkSession);
        Dataset<Row> listingDs = getListingDs(sparkSession);
        avgNumReviewPerMonth(reviewsDs, listingDs);
        //reviewsDs.show();

        /*
        avgCityPrice(listingDs);
        avgRoomTypePrice(listingDs);*/

        //distinctListings(listingDs);

        /*
        ds.select("price")
                .map( row -> Float.valueOf(
                            ((String) row.getAs("price"))
                            .replace("$", "")
                            .replace(",", ""))

                , Encoders.FLOAT() )
                .filter( functions.col("value").gt(0) )
                .agg( functions.avg("value") )
                .show();*/

        //System.out.println(val);

        //ds.show();
        /*

        Dataset<Row> priceAvg = ds
                .filter( functions.col("price").notEqual(0) )
                .agg( functions.avg("price") );

        priceAvg.show();
*/
    }

}

