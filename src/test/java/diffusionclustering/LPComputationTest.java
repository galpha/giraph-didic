package diffusionclustering;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.utils.InternalVertexRunner;
import org.junit.Test;

import java.util.regex.Pattern;

/**
 * Test cases for {@link DiffusionComputation}.
 */
public class LPComputationTest {

  @Test
  public void testConnectedGraph() throws Exception {
    String[] graph = GiraphTestHelper.getMySuperSimpleGraph();
    computeResults(graph);
  }

  private void computeResults(String[] graph) throws Exception {
    GiraphConfiguration conf = getConfiguration();
    Iterable<String> results = InternalVertexRunner.run(conf, graph);
    for (String line : results) System.out.println(line);
  }

  private GiraphConfiguration getConfiguration() {
    GiraphConfiguration conf = new GiraphConfiguration();
    conf.setComputationClass(DiffusionComputation.class);
    conf.setMasterComputeClass(DiffusionMasterComputation.class);
    conf.setVertexInputFormatClass(DiffusionTextVertexInputFormat.class);
    conf.setVertexOutputFormatClass(DiffusionTextVertexOutputFormat.class);
    return conf;
  }
}
