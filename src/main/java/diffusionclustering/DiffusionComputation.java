package diffusionclustering;

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
  public static final double DEFAULT_SECONDARY_LOAD_FACTOR = 10;
  public static final String EDGE_FLOW_SCALE = "diffusion.edgeflowscale";
  public static final double DEFAULT_EDGE_FLOW_SCALE = 0.5;
  public static final String NUMBER_OF_CLUSTERS = "diffusion.cluster.num";
  public static final int DEFAULT_NUMBER_OF_CLUSTERS = 2;
  private int k;
  private double edgeFlowScale;
  private double secondaryLoadFactor;

  /**
   * {@inheritDoc}
   */
  @Override
  public void initialize(GraphState graphState,
    WorkerClientRequestProcessor<LongWritable, DiffusionVertexValue,
      NullWritable> workerClientRequestProcessor,
    GraphTaskManager<LongWritable, DiffusionVertexValue, NullWritable>
      graphTaskManager,
    WorkerGlobalCommUsage workerGlobalCommUsage, WorkerContext workerContext) {
    this.k = getConf().getInt(NUMBER_OF_CLUSTERS, DEFAULT_NUMBER_OF_CLUSTERS);
    this.edgeFlowScale =
      getConf().getDouble(EDGE_FLOW_SCALE, DEFAULT_EDGE_FLOW_SCALE);
    this.secondaryLoadFactor =
      getConf().getDouble(SECONDARY_LOAD_FACTOR, DEFAULT_SECONDARY_LOAD_FACTOR);
    super.initialize(graphState, workerClientRequestProcessor, graphTaskManager,
      workerGlobalCommUsage, workerContext);
  }

  /**
   * Setting start cluster
   *
   * @param vertex current Vertex
   */
  private void setStartCluster(
    Vertex<LongWritable, DiffusionVertexValue, NullWritable> vertex) {
    int startingCluster = (int) (vertex.getId().get() % k);
    vertex.getValue().setCurrentCluster(new IntWritable(startingCluster));
  }

  /**
   * Setting start load
   *
   * @param vertex current Vertex
   */
  private void setStartLoad(
    Vertex<LongWritable, DiffusionVertexValue, NullWritable> vertex) {
    ArrayList<Double> primaryLoad = new ArrayList<>();
    ArrayList<Double> secondaryLoad = new ArrayList<>();
    for (int i = 0; i < k; i++) {
      if (i == vertex.getValue().getCurrentCluster().get()) {
        primaryLoad.add(1.0);
        secondaryLoad.add(1.0);
      } else {
        primaryLoad.add(0.0);
        secondaryLoad.add(0.0);
      }
    }
    vertex.getValue().setPrimaryLoad(primaryLoad);
    vertex.getValue().setSecondaryLoad(secondaryLoad);
  }

  private void calculateNewSecondaryLoad(
    Vertex<LongWritable, DiffusionVertexValue, NullWritable> vertex,
    List<List<Double>> secondaryLoads, List<Integer> neighborClusters) {
    for (int i = 0; i < vertex.getValue().getSecondaryLoad().size(); i++) {
      double vertexLoad = vertex.getValue().getSecondaryLoad().get(i);
      double newLoad = 0.0;
      for (int j = 0; j < secondaryLoads.size(); j++) {
        double neighborLoad = secondaryLoads.get(j).get(i);
        double vertexModifier = 1.0;
        double neighborModifier = 1.0;
        if (i == vertex.getValue().getCurrentCluster().get()) {
          vertexModifier = secondaryLoadFactor;
        }
        if (i == neighborClusters.get(j)) {
          neighborModifier = secondaryLoadFactor;
        }
        newLoad +=
          ((neighborLoad / neighborModifier) - (vertexLoad / vertexModifier)) *
            edgeFlowScale;
        vertexLoad += newLoad;
        vertex.getValue().getSecondaryLoad().set(i, vertexLoad);
      }
    }
  }

  private void calculateNewPrimaryLoad(
    Vertex<LongWritable, DiffusionVertexValue, NullWritable> vertex,
    List<List<Double>> primaryLoads) {
    for (int i = 0; i < vertex.getValue().getPrimaryLoad().size(); i++) {
      double vertexLoad = vertex.getValue().getPrimaryLoad().get(i);
      double newLoad = 0.0;
      for (List<Double> neighborPrimaryLoad : primaryLoads) {
        newLoad += (neighborPrimaryLoad.get(i) - vertexLoad) * edgeFlowScale;
      }
      vertexLoad += newLoad;
      vertexLoad += vertex.getValue().getSecondaryLoad().get(i);
      vertex.getValue().getPrimaryLoad().set(i, vertexLoad);
    }
  }

  public void determineNewCluster(
    Vertex<LongWritable, DiffusionVertexValue, NullWritable> vertex) {
    int cluster = 0;
    Double maxLoad = 0.0;
    for (int i = 0; i < vertex.getValue().getPrimaryLoad().size(); i++) {
      Double load = vertex.getValue().getPrimaryLoad().get(i);
      if (load > maxLoad) {
        maxLoad = load;
        cluster = i;
      }
    }
    vertex.getValue().setCurrentCluster(new IntWritable(cluster));
  }

  @Override
  public void compute(
    Vertex<LongWritable, DiffusionVertexValue, NullWritable> vertex,
    Iterable<DiffusionVertexValue> messages) throws IOException {
    if (getSuperstep() == 0) {
      setStartCluster(vertex);
      setStartLoad(vertex);
      sendMessageToAllEdges(vertex, vertex.getValue());
    } else {
      List<List<Double>> primaryLoadMessages = new ArrayList<>();
      List<List<Double>> secondaryLoadMessages = new ArrayList<>();
      List<Integer> neighborClusters = new ArrayList<>();
      for (DiffusionVertexValue neighborValue : messages) {
        primaryLoadMessages.add(neighborValue.getPrimaryLoad());
        secondaryLoadMessages.add(neighborValue.getSecondaryLoad());
        neighborClusters.add(neighborValue.getCurrentCluster().get());
      }
      calculateNewSecondaryLoad(vertex, secondaryLoadMessages,
        neighborClusters);
      calculateNewPrimaryLoad(vertex, primaryLoadMessages);
//      List<Double> newSecondaryLoad = calculateNewSecondaryLoads(
//        Lists.newArrayList(vertex.getValue().getSecondaryLoad()),
//        vertex.getValue().getCurrentCluster().get(), secondaryLoadMessages,
//        neighborClusters);
//      vertex.getValue().setSecondaryLoad(newSecondaryLoad);
//      vertex.getValue().setPrimaryLoad(calculateNewPrimaryLoad(
//        Lists.newArrayList(vertex.getValue().getPrimaryLoad()),
//        primaryLoadMessages, newSecondaryLoad));
      if (getSuperstep() % 10 == 0) {
        determineNewCluster(vertex);
      }
      sendMessageToAllEdges(vertex, vertex.getValue());
    }
    vertex.voteToHalt();
  }
//  public List<Double> calculateNewPrimaryLoad(List<Double> primaryLoads,
//    List<List<Double>> primaryLoadMessages, List<Double> secondaryLoads) {
//    for (int i = 0; i < primaryLoads.size(); i++) {
//      Double ownLoad = primaryLoads.get(i);
//      Double loadChange = 0.0;
//      for (List<Double> message : primaryLoadMessages) {
//        loadChange += (message.get(i) - ownLoad) * edgeFlowScale;
//      }
//      ownLoad -= loadChange;
//      ownLoad += secondaryLoads.get(i);
//      primaryLoads.set(i, ownLoad);
//    }
//    return primaryLoads;
//  }
//  public List<Double> calculateNewSecondaryLoads(List<Double> secondaryLoads,
//    int currentCluster, List<List<Double>> secondaryLoadMessages,
//    List<Integer> messageClusters) {
//
//    for (int i = 0; i < secondaryLoads.size(); i++) {
//      Double ownLoad = secondaryLoads.get(i);
//      Double loadChange = 0.0;
//      for (int j = 0; j < secondaryLoadMessages.size(); j++) {
//        Double msgLoad = secondaryLoadMessages.get(j).get(i);
//        Double ownModifier = 1.0;
//        Double msgModifier = 1.0;
//        if (i == currentCluster) {
//          ownModifier = secondaryLoadFactor;
//        }
//        if (i == messageClusters.get(j)) {
//          msgModifier = secondaryLoadFactor;
//        }
//        loadChange +=
//          ((msgLoad / msgModifier) - (ownLoad / ownModifier)) * edgeFlowScale;
//      }
//      ownLoad -= loadChange;
//      secondaryLoads.set(i, ownLoad);
//    }
//    return secondaryLoads;
//  }
}
