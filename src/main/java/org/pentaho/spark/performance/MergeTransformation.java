package org.pentaho.spark.performance;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class MergeTransformation {

  public static void main( String[] args ) throws Exception {
    log.info( "main(args: {})", args );

    if ( args.length != 2 ) {
      throw new RuntimeException( "FilterTransformation requires 2 input parameters; an input and output folder." );
    }

    String input = args[ 0 ];
    String output = args[ 1 ];

    SparkSession spark = SparkSession.builder().getOrCreate();

    MergeTransformation transformation = new MergeTransformation( spark );
    transformation.run( input, output );
  }

  private SparkSession spark;

  public MergeTransformation( SparkSession spark ) {
    this.spark = spark;
  }

  public void run( String input, String output ) {
    log.info( "run(input: {}, output: {})", input, output );
    Dataset<Row> ds = spark.read()
      .option( "header", "true" )
      .option( "delimiter", "\t" )
      .option( "inferSchema", "true" )
      .csv( input );

    Dataset<Row> grid = dataGrid();

    Column criteria = ds.col( "star_rating" ).equalTo( grid.col( "star_id" ));
    String joinType = "INNER";

    Dataset<Row> result = ds.join( grid, criteria, joinType );

    result.write().csv( output );

    spark.stop();
  }

  private Dataset<Row> dataGrid(){
    StructField[] fields = new StructField[]{
      DataTypes.createStructField( "star_id", DataTypes.IntegerType, true ),
      DataTypes.createStructField( "star_name", DataTypes.StringType, true ),
    };

    List<Row> rows = new ArrayList<>(  );
    rows.add( RowFactory.create(4, "Four Stars"));
    rows.add( RowFactory.create(5, "Five Stars"));

    StructType schema = DataTypes.createStructType(fields);
    Dataset<Row> dataGrid = spark.createDataset( rows, RowEncoder.apply(schema) );
    return dataGrid;
  }

}
