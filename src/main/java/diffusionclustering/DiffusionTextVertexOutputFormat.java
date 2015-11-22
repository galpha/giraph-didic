package diffusionclustering;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class DiffusionTextVertexOutputFormat extends
  TextVertexOutputFormat<LongWritable, DiffusionVertexValue, NullWritable> {
  private static final String VALUE_TOKEN_SEPARATOR = " ";

  @Override
  public TextVertexWriter createVertexWriter(TaskAttemptContext context) throws
    IOException, InterruptedException {
    return new DiffusionTextVertexLineWriter();
  }

  private class DiffusionTextVertexLineWriter extends
    TextVertexWriterToEachLine {
    @Override
    protected Text convertVertexToLine(
      Vertex<LongWritable, DiffusionVertexValue, NullWritable> vertex) throws
      IOException {
      // vertex id
      StringBuilder sb = new StringBuilder(vertex.getId().toString());
      sb.append(VALUE_TOKEN_SEPARATOR);
      // vertex value
      sb.append(vertex.getValue().getCurrentCluster());
      sb.append(VALUE_TOKEN_SEPARATOR);
      Iterable<Double> primaryLoad = vertex.getValue().getPrimaryLoad();
      for (Double load : primaryLoad) {
        sb.append(load);
        sb.append(VALUE_TOKEN_SEPARATOR);
      }
      return new Text(sb.toString());
    }
  }
}
