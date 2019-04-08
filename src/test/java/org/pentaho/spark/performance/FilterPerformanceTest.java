package org.pentaho.spark.performance;

import org.apache.spark.sql.SparkSession;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FilterPerformanceTest extends SparkTest {

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();


  @Test
  public void test() {

    String input = FilterPerformanceTest.class.getClassLoader().getResource( "software.tsv" ).getFile();
    String output = tempFolder.getRoot().getAbsolutePath();


    FilterTransformation transformation = new FilterTransformation( spark() );
    transformation.run( input, output );

    File verifiedFile = new File( output + "/verified" );
    File unverifiedFile = new File( output + "/unverified" );

    assertTrue( verifiedFile.exists() );
    assertTrue( unverifiedFile.exists() );

    SparkSession sparkAssert = SparkSession.builder().master( "local[*]" ).getOrCreate();
    long allCount = sparkAssert.read().option("header","true").csv(input).count();
    long verifiedCount = sparkAssert.read().csv(verifiedFile.getAbsolutePath()).count();
    long unverifiedCount = sparkAssert.read().csv(unverifiedFile.getAbsolutePath()).count();

    assertEquals(allCount, verifiedCount + unverifiedCount);
    sparkAssert.stop();
  }

}
