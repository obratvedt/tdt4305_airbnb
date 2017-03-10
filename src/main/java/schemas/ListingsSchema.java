package schemas;


import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class ListingsSchema {
    //TODO: Add the correct fields from the file that are relevant to the task (~100 fields are way to much to add)
    public static StructType getListingsSchema() {
        StructType listingsSchema = DataTypes
                .createStructType(new StructField[]{
                        DataTypes.createStructField("listing_id", DataTypes.StringType, true),
                        DataTypes.createStructField("price", DataTypes.FloatType, true)
                });


        return listingsSchema;
    }
}
