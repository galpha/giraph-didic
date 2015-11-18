package diffusionclustering;

import com.google.common.collect.Lists;
import org.apache.giraph.comm.WorkerClientRequestProcessor;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.GraphState;
import org.apache.giraph.graph.GraphTaskManager;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.worker.WorkerContext;
import org.apache.giraph.worker.WorkerGlobalCommUsage;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DiffusionComputation extends
  BasicComputation<LongWritable, DiffusionVertexValue, NullWritable,
    DiffusionVertexValue> {
  public static final String NUMBER_OF_CLUSTERS = "diffusion.numberofclusters";
  public static final int DEFAULT_NUMBER_OF_CLUSTERS = 5;
  public static final String SECONDARY_LOAD_FACTOR =
    "diffusion" + ".secondaryloadfactor";
  public static final int DEFAULT_SECONDARY_LOAD_FACTOR = 10;
  public static int numberOfClusters;

  @Override
  public void initialize(GraphState graphState,
    WorkerClientRequestProcessor<LongWritable, DiffusionVertexValue,
      NullWritable> workerClientRequestProcessor,
    GraphTaskManager<LongWritable, DiffusionVertexValue, NullWritable>
      graphTaskManager,
    WorkerGlobalCommUsage workerGlobalCommUsage, WorkerContext workerContext) {
    super.initialize(graphState, workerClientRequestProcessor, graphTaskManager,
      workerGlobalCommUsage, workerContext);
    numberOfClusters =
      getConf().getInt(NUMBER_OF_CLUSTERS, DEFAULT_NUMBER_OF_CLUSTERS);
  }

  @Override
  public void compute(
    Vertex<LongWritable, DiffusionVertexValue, NullWritable> vertex,
    Iterable<DiffusionVertexValue> messages) throws IOException {
    DiffusionVertexValue vertexValue = vertex.getValue();
    if (getSuperstep() == 0) {
      sendMessageToAllEdges(vertex, vertex.getValue());
    } else {
      List<List<Long>> primaryLoadMessages = new ArrayList<>();
      List<List<Long>> secondaryLoadMessages = new ArrayList<>();
      List<Integer> messageClusters = new ArrayList<>();
      for (DiffusionVertexValue message : messages) {
        List<Long> primary = Lists.newArrayList(message.getPrimaryLoad());
        primaryLoadMessages.add(primary);
        List<Long> secondary = Lists.newArrayList(message.getSecondaryLoad());
        secondaryLoadMessages.add(secondary);
        int cluster = message.getCurrentCluster().get();
        messageClusters.add(cluster);
      }
      computeNewPrimaryLoads(Lists.newArrayList(vertexValue.getPrimaryLoad()),
        primaryLoadMessages);
      computeNewSecondaryLoads(
        Lists.newArrayList(vertexValue.getSecondaryLoad()),
        vertexValue.getCurrentCluster().get(), secondaryLoadMessages,
        messageClusters);
      if (getSuperstep() % 10 == 0) {
        vertex.getValue().setCurrentCluster(new IntWritable(determineNewCluster(
          Lists.newArrayList(vertexValue.getPrimaryLoad()))));
      }
      sendMessageToAllEdges(vertex, vertex.getValue());
    }
    vertex.voteToHalt();
  }

  public int determineNewCluster(List<Long> primaryLoad) {
    int cluster = 0;
    long maxLoad = 0;
    for (int i = 0; i < primaryLoad.size(); i++) {
      Long load = primaryLoad.get(i);
      if (load > maxLoad) {
        maxLoad = load;
        cluster = i;
      }
    }
    return cluster;
  }

  public List<Long> computeNewPrimaryLoads(List<Long> primaryLoads,
    List<List<Long>> primaryLoadMessages) {
    for(int i=0;i<primaryLoads.size();i++){
      Long ownLoad = primaryLoads.get(i);
      Long loadChange = 0L;
      for(List<Long> message : primaryLoadMessages){
        loadChange -= (ownLoad - message.get(i));
      }
      ownLoad += loadChange;
      primaryLoads.set(i, ownLoad); // add edge weight here
    }
    return primaryLoads;
  }

  public List<Long> computeNewSecondaryLoads(List<Long> secondaryLoads,
    int currentCluster, List<List<Long>> secondaryLoadMessages,
    List<Integer> messageClusters) {
    
    return secondaryLoads;
  }
}
