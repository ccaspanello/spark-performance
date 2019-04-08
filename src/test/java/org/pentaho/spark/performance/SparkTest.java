package org.pentaho.spark.performance;

import org.apache.spark.sql.SparkSession;
import org.junit.Before;

public class SparkTest {

  private SparkSession spark;

  @Before
  public void before(){
    spark = SparkSession.builder().master( "local[*]" ).getOrCreate();
  }

  protected SparkSession spark(){
    return spark;
  }
}
