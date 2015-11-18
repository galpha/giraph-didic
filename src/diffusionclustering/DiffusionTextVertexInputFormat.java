package diffusionclustering;

import com.google.common.collect.Lists;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

public class DiffusionTextVertexInputFormat extends
  TextVertexInputFormat<LongWritable, DiffusionVertexValue, NullWritable> {

  private static final Pattern SEPARATOR = Pattern.compile("[\t ]");

  @Override
  public TextVertexReader createVertexReader(InputSplit split,
    TaskAttemptContext context) throws IOException {
    return new VertexReader();
  }

  public class VertexReader extends
    TextVertexReaderFromEachLineProcessed<String[]> {

    private int id;
    private int clusterCount = DiffusionComputation.DEFAULT_NUMBER_OF_CLUSTERS;

    @Override
    protected String[] preprocessLine(Text line) throws IOException {
      String[] tokens = SEPARATOR.split(line.toString());
      id = Integer.parseInt(tokens[0]);
      return tokens;
    }

    @Override
    protected LongWritable getId(String[] tokens) throws IOException {
      return new LongWritable(id);
    }

    @Override
    protected DiffusionVertexValue getValue(String[] tokens) throws IOException {
      int startingCluster = id % clusterCount;
      return new DiffusionVertexValue(clusterCount, startingCluster);
    }

    @Override
    protected Iterable<Edge<LongWritable, NullWritable>> getEdges(
      String[] tokens) throws IOException {
      List<Edge<LongWritable, NullWritable>> edges = Lists.newArrayList();
      for (int n = 2; n < tokens.length; n++) {
        edges
          .add(EdgeFactory.create(new LongWritable(Long.parseLong(tokens[n]))));
      }
      return edges;
    }
  }
}
