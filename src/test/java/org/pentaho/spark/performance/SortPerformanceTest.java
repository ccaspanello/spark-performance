package org.pentaho.spark.performance;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;

import static org.junit.Assert.assertTrue;

public class SortPerformanceTest extends SparkTest  {

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();


  @Test
  public void test() {

    String input = SortPerformanceTest.class.getClassLoader().getResource( "software.tsv" ).getFile();
    File output = new File(tempFolder.getRoot().getAbsolutePath(), "sort");

    SortTransformation transformation = new SortTransformation( spark() );
    transformation.run( input, output.getAbsolutePath() );

    assertTrue( output.exists() );

  }

}
