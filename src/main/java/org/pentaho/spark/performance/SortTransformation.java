package org.pentaho.spark.performance;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

@Slf4j
public class SortTransformation {

  public static void main( String[] args ) throws Exception {
    log.info( "main(args: {})", args );

    if ( args.length != 2 ) {
      throw new RuntimeException( "FilterTransformation requires 2 input parameters; an input and output folder." );
    }

    String input = args[ 0 ];
    String output = args[ 1 ];

    SparkSession spark = SparkSession.builder().getOrCreate();

    SortTransformation transformation = new SortTransformation( spark );
    transformation.run( input, output );
  }

  private SparkSession spark;

  public SortTransformation( SparkSession spark ) {
    this.spark = spark;
  }

  public void run( String input, String output ) {
    log.info( "run(input: {}, output: {})", input, output );
    Dataset<Row> ds = spark.read()
      .option( "header", "true" )
      .option( "delimiter", "\t" )
      .option( "inferSchema", "true" )
      .csv( input );

    Dataset<Row> result = ds.sort( ds.col( "customer_id" ), ds.col( "star_rating" ) );

    result.write().csv( output );

    spark.stop();
  }

}
