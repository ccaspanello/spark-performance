package org.pentaho.spark.performance;

import org.apache.spark.sql.SparkSession;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GroupByPerformanceTest extends SparkTest  {

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();


  @Test
  public void test() {

    String input = GroupByPerformanceTest.class.getClassLoader().getResource( "software.tsv" ).getFile();
    File output = new File(tempFolder.getRoot().getAbsolutePath(), "groupBy");

    GroupByTransformation transformation = new GroupByTransformation( spark() );
    transformation.run( input, output.getAbsolutePath() );

    assertTrue( output.exists() );

  }

}
