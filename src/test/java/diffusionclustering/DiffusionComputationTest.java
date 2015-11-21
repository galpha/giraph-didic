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
    validateResults(computeResults(graph));
  }

  private void validateResults(Map<Integer, List<Double>> vertexIDwithValue) {
    assertEquals(2, vertexIDwithValue.size());
    assertEquals(0.0, vertexIDwithValue.get(0).get(0), 0);
    assertEquals(1.45, vertexIDwithValue.get(0).get(1), 0);
    assertEquals(0.55, vertexIDwithValue.get(0).get(2), 0);
    assertEquals(1.0, vertexIDwithValue.get(1).get(0), 0);
    assertEquals(0.0, vertexIDwithValue.get(1).get(1), 0);
    assertEquals(1.0, vertexIDwithValue.get(1).get(2), 0);
  }

  private GiraphConfiguration getConfiguration() {
    GiraphConfiguration conf = new GiraphConfiguration();
    conf.setComputationClass(DiffusionComputation.class);
    conf.setMasterComputeClass(DiffusionMasterComputation.class);
    conf.setVertexInputFormatClass(DiffusionTextVertexInputFormat.class);
    conf.setVertexOutputFormatClass(DiffusionTextVertexOutputFormat.class);
    return conf;
  }

  private Map<Integer, List<Double>> computeResults(String[] graph) throws
    Exception {
    GiraphConfiguration conf = getConfiguration();
    Iterable<String> results = InternalVertexRunner.run(conf, graph);
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
      values.add(Double.valueOf(lineTokens[1]));
      values.add(Double.valueOf(lineTokens[2]));
      values.add(Double.valueOf(lineTokens[3]));
      parsedResults.put(vertexID, values);
    }
    return parsedResults;
  }
}
