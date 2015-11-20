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
  public static final String SECONDARY_LOAD_FACTOR =
    "diffusion.secondaryloadfactor";
  public static final int DEFAULT_SECONDARY_LOAD_FACTOR = 10;
  public static final String EDGE_FLOW_SCALE = "diffusion.edgeflowscale";
  public static final double DEFAULT_EDGE_FLOW_SCALE = 0.5;
  public static final String NUMBER_OF_CLUSTERS = "diffusion.numberofclusters";
  public static final int DEFAULT_NUMBER_OF_CLUSTERS = 2;

  @Override
  public void initialize(GraphState graphState,
    WorkerClientRequestProcessor<LongWritable, DiffusionVertexValue,
      NullWritable> workerClientRequestProcessor,
    GraphTaskManager<LongWritable, DiffusionVertexValue, NullWritable>
      graphTaskManager,
    WorkerGlobalCommUsage workerGlobalCommUsage, WorkerContext workerContext) {
    super.initialize(graphState, workerClientRequestProcessor, graphTaskManager,
      workerGlobalCommUsage, workerContext);
  }

  @Override
  public void compute(
    Vertex<LongWritable, DiffusionVertexValue, NullWritable> vertex,
    Iterable<DiffusionVertexValue> messages) throws IOException {
    DiffusionVertexValue vertexValue = vertex.getValue();
    if (getSuperstep() == 0) {
      int clusterCount =
        getConf().getInt(NUMBER_OF_CLUSTERS, DEFAULT_NUMBER_OF_CLUSTERS);
      int startingCluster = (int) (vertex.getId().get() % clusterCount);
      ArrayList<Double> primaryLoad = new ArrayList<>();
      ArrayList<Double> secondaryLoad = new ArrayList<>();
      for (int i = 0; i < clusterCount; i++) {
        if (i != startingCluster) {
          primaryLoad.add(0.0);
          secondaryLoad.add(0.0);
        } else {
          primaryLoad.add(1.0);
          secondaryLoad.add(1.0);
        }
      }
      vertex.getValue().setPrimaryLoad(primaryLoad);
      vertex.getValue().setSecondaryLoad(secondaryLoad);
      vertex.getValue().setCurrentCluster(new IntWritable(startingCluster));
      sendMessageToAllEdges(vertex, vertex.getValue());
    } else {
      for(DiffusionVertexValue m : messages){
        System.out.println(m.getPrimaryLoad() + " asdf");
        System.out.println(m.getSecondaryLoad());
      }
      List<List<Double>> primaryLoadMessages = new ArrayList<>();
      List<List<Double>> secondaryLoadMessages = new ArrayList<>();
      List<Integer> neighborCluster = new ArrayList<>();
      int l = 0;
      for (DiffusionVertexValue message : messages) {
        List<Double> primary = Lists.newArrayList(message.getPrimaryLoad());
        primaryLoadMessages.add(primary);
        List<Double> secondary = Lists.newArrayList(message.getSecondaryLoad());
        secondaryLoadMessages.add(secondary);
        int cluster = message.getCurrentCluster().get();
        neighborCluster.add(cluster);
      }
      List<Double> newSecondaryLoad = computeNewSecondaryLoads(
        Lists.newArrayList(vertexValue.getSecondaryLoad()),
        vertexValue.getCurrentCluster().get(), secondaryLoadMessages,
        neighborCluster);
      vertex.getValue().setSecondaryLoad(newSecondaryLoad);
      vertex.getValue().setPrimaryLoad(
        computeNewPrimaryLoads(Lists.newArrayList(vertexValue.getPrimaryLoad()),
          primaryLoadMessages, newSecondaryLoad));
      if (getSuperstep() % 10 == 0) {
        vertex.getValue().setCurrentCluster(new IntWritable(determineNewCluster(
          Lists.newArrayList(vertexValue.getPrimaryLoad()))));
      }
      sendMessageToAllEdges(vertex, vertex.getValue());
    }
    vertex.voteToHalt();
  }

  public int determineNewCluster(List<Double> primaryLoad) {
    int cluster = 0;
    Double maxLoad = 0.0;
    for (int i = 0; i < primaryLoad.size(); i++) {
      Double load = primaryLoad.get(i);
      if (load > maxLoad) {
        maxLoad = load;
        cluster = i;
      }
    }
    return cluster;
  }

  public List<Double> computeNewPrimaryLoads(List<Double> primaryLoads,
    List<List<Double>> primaryLoadMessages, List<Double> secondaryLoads) {
    for (int i = 0; i < primaryLoads.size(); i++) {
      Double ownLoad = primaryLoads.get(i);
      Double loadChange = 0.0;
      for (List<Double> message : primaryLoadMessages) {
        loadChange += (message.get(i) - ownLoad) *
          getConf().getDouble(EDGE_FLOW_SCALE, DEFAULT_EDGE_FLOW_SCALE);
      }
      ownLoad -= loadChange;
      ownLoad += secondaryLoads.get(i);
      primaryLoads.set(i, ownLoad);
    }
    return primaryLoads;
  }

  public List<Double> computeNewSecondaryLoads(List<Double> secondaryLoads,
    int currentCluster, List<List<Double>> secondaryLoadMessages,
    List<Integer> messageClusters) {
    Double secondaryLoadFactor =
      getConf().getDouble(SECONDARY_LOAD_FACTOR, DEFAULT_SECONDARY_LOAD_FACTOR);
    for (int i = 0; i < secondaryLoads.size(); i++) {
      Double ownLoad = secondaryLoads.get(i);
      Double loadChange = 0.0;
      for (int j = 0; j < secondaryLoadMessages.size(); j++) {
        Double msgLoad = secondaryLoadMessages.get(j).get(i);
        Double ownModifier = 1.0;
        Double msgModifier = 1.0;
        if (i == currentCluster) {
          ownModifier = secondaryLoadFactor;
        }
        if (i == messageClusters.get(j)) {
          msgModifier = secondaryLoadFactor;
        }
        loadChange += ((msgLoad / msgModifier) - (ownLoad / ownModifier))*
          getConf().getDouble(EDGE_FLOW_SCALE, DEFAULT_EDGE_FLOW_SCALE);
      }
      ownLoad -= loadChange;
      secondaryLoads.set(i, ownLoad);
    }
    return secondaryLoads;
  }
}
