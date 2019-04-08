package org.pentaho.spark.performance;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

@Slf4j
public class FilterTransformation {

  public static void main( String[] args ) throws Exception {
    log.info("main(args: {})", args);

    if ( args.length != 2 ) {
      throw new RuntimeException( "FilterTransformation requires 2 input parameters; an input and output folder." );
    }

    String input = args[ 0 ];
    String output = args[ 1 ];

    SparkSession spark = SparkSession.builder().getOrCreate();

    FilterTransformation transformation = new FilterTransformation( spark );
    transformation.run( input, output );
  }

  private SparkSession spark;

  public FilterTransformation( SparkSession spark ) {
    this.spark = spark;
  }

  public void run( String input, String output ) {
    log.info("run(input: {}, output: {})", input, output);
    Dataset<Row> ds = spark.read()
      .option( "header", "true" )
      .option( "delimiter", "\t" )
      .option( "inferSchema", "true" )
      .csv( input );

    Dataset<Row> verified = ds.filter( ds.col( "verified_purchase" ).equalTo( "Y" ) );
    Dataset<Row> unverified = ds.filter( ds.col( "verified_purchase" ).equalTo( "N" ) );

    verified.write().csv(output+"/verified");
    unverified.write().csv(output+"/unverified");

    spark.stop();
  }

  /**
   * Using .schema( schema() ) did not work for some reason.
   * @return
   */
  private StructType schema() {
    StructField[] structFields = new StructField[] {
      DataTypes.createStructField( "marketplace", DataTypes.StringType, false ),
      DataTypes.createStructField( "customer_id", DataTypes.StringType, false ),
      DataTypes.createStructField( "review_id", DataTypes.StringType, false ),
      DataTypes.createStructField( "product_id", DataTypes.StringType, false ),
      DataTypes.createStructField( "product_parent", DataTypes.StringType, false ),
      DataTypes.createStructField( "product_title", DataTypes.StringType, false ),
      DataTypes.createStructField( "product_category", DataTypes.StringType, false ),
      DataTypes.createStructField( "star_rating", DataTypes.IntegerType, false ),
      DataTypes.createStructField( "helpful_votes", DataTypes.IntegerType, false ),
      DataTypes.createStructField( "total_votes", DataTypes.IntegerType, false ),
      DataTypes.createStructField( "vine", DataTypes.BooleanType, false ),
      DataTypes.createStructField( "verified_purchase", DataTypes.StringType, false ),
      DataTypes.createStructField( "review_headline", DataTypes.StringType, false ),
      DataTypes.createStructField( "review_body", DataTypes.StringType, false ),
      DataTypes.createStructField( "review_date", DataTypes.DateType, false )
    };
    return DataTypes.createStructType( structFields );
  }

}
