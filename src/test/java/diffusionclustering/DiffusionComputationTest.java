package diffusionclustering;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.utils.InternalVertexRunner;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;

/**
 * Test cases for {@link DiffusionComputation}.
 */
public class DiffusionComputationTest {
  private static final Pattern LINE_TOKEN_SEPARATOR = Pattern.compile(" ");

  @Test
  public void testDirectedGraphFrom1To0() throws Exception {
    String[] graph = GiraphTestHelper.getDirectedGraphFrom1To0();
    Map<Integer, List<Double>> results = computeResults(graph);
    validateResultsFrom1To0(results);
  }

  @Test
  public void testDirectedGraphFrom0To1() throws Exception {
    String[] graph = GiraphTestHelper.getDirectedGraphFrom0To1();
    Map<Integer, List<Double>> results = computeResults(graph);
    validateResultsFrom0To1(results);
  }

  @Test
  public void testDirected4NodeGraph() throws Exception {
    String[] graph = GiraphTestHelper.getDirected4NodeGraph();
    Map<Integer, List<Double>> results = computeResults(graph);
    validateResults4Nodes(results);
  }

  private void validateResultsFrom1To0(Map<Integer, List<Double>> vertexIDwithValue) {
    assertEquals(2, vertexIDwithValue.size());
    assertEquals(0.0, vertexIDwithValue.get(0).get(0), 0);
    assertEquals(1.45, vertexIDwithValue.get(0).get(1), 0);
    assertEquals(0.55, vertexIDwithValue.get(0).get(2), 0);
    assertEquals(1.0, vertexIDwithValue.get(1).get(0), 0);
    assertEquals(0.0, vertexIDwithValue.get(1).get(1), 0);
    assertEquals(1.0, vertexIDwithValue.get(1).get(2), 0);
  }

  private void validateResultsFrom0To1(Map<Integer, List<Double>> vertexIDwithValue) {
    assertEquals(2, vertexIDwithValue.size());
    assertEquals(0.0, vertexIDwithValue.get(0).get(0), 0);
    assertEquals(1.0, vertexIDwithValue.get(0).get(1), 0);
    assertEquals(0.0, vertexIDwithValue.get(0).get(2), 0);
    assertEquals(1.0, vertexIDwithValue.get(1).get(0), 0);
    assertEquals(0.55, vertexIDwithValue.get(1).get(1), 0);
    assertEquals(1.45, vertexIDwithValue.get(1).get(2), 0);
  }

  // TODO: validate values manually
  private void validateResults4Nodes(Map<Integer, List<Double>> vertexIDwithValue) {
    assertEquals(4, vertexIDwithValue.size());
    assertEquals(0.0, vertexIDwithValue.get(0).get(0), 0);
    assertEquals(1.45, vertexIDwithValue.get(0).get(1), 0);
    assertEquals(0.55, vertexIDwithValue.get(0).get(2), 0);
    assertEquals(1.0, vertexIDwithValue.get(1).get(0), 0);
    assertEquals(2.3, vertexIDwithValue.get(1).get(1), 0.1);
    assertEquals(1.5, vertexIDwithValue.get(1).get(2), 0.1);
    assertEquals(0.0, vertexIDwithValue.get(2).get(0), 0);
    assertEquals(2.7, vertexIDwithValue.get(2).get(1), 0.1);
    assertEquals(0.3, vertexIDwithValue.get(2).get(2), 0.1);
    assertEquals(1.0, vertexIDwithValue.get(3).get(0), 0);
    assertEquals(0.0, vertexIDwithValue.get(3).get(1), 0);
    assertEquals(1.0, vertexIDwithValue.get(3).get(2), 0);
  }

  private GiraphConfiguration getConfiguration() {
    GiraphConfiguration conf = new GiraphConfiguration();
    conf.setComputationClass(DiffusionComputation.class);
    conf.setMasterComputeClass(DiffusionMasterComputation.class);
    conf.setVertexInputFormatClass(DiffusionTextVertexInputFormat.class);
    conf.setVertexOutputFormatClass
      (DiffusionTextVertexOutputFormat.class);
    conf.setBoolean(DiffusionTextVertexOutputFormat.TEST_OUTPUT, true);
    return conf;
  }

  private Map<Integer, List<Double>> computeResults(String[] graph) throws
    Exception {
    GiraphConfiguration conf = getConfiguration();
    Iterable<String> results = InternalVertexRunner.run(conf, graph);
    for (String result: results){
      System.out.println(result);
    }
    return parseResults(results);
  }

  private Map<Integer, List<Double>> parseResults(Iterable<String> results) {
    Map<Integer, List<Double>> parsedResults = Maps.newHashMap();
    String[] lineTokens;
    int vertexID;
    for (String line : results) {
      lineTokens = LINE_TOKEN_SEPARATOR.split(line);
      vertexID = Integer.parseInt(lineTokens[0]);
      List<Double> values = Lists.newArrayList();
      for(int i=1;i<lineTokens.length;i++){

        values.add(Double.valueOf(lineTokens[i]));
      }
      parsedResults.put(vertexID, values);
    }
    return parsedResults;
  }
}
