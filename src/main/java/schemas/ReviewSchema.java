package schemas;


import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class ReviewSchema {

    public static StructType getReviewSchema() {

        StructType reviewSchema = DataTypes
                .createStructType(new StructField[]{
                        DataTypes.createStructField("listing_id", DataTypes.StringType, true),
                        DataTypes.createStructField("id", DataTypes.StringType, true),
                        DataTypes.createStructField("date", DataTypes.StringType, true),
                        DataTypes.createStructField("reviewer_id", DataTypes.StringType, true),
                        DataTypes.createStructField("reviewer_name", DataTypes.StringType, true),
                        DataTypes.createStructField("comment", DataTypes.StringType, true)
                });
        return reviewSchema;
    }
}
