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

/**
 * @author Niklas Teichmann (teichmann@informatik.uni-leipzig.de)
 * @author Stefan Faulhaber (faulhaber@informatik.uni-leipzig.de)
 * @author Kevin Gomez (gomez@studserv.uni-leipzig.de)
 */
public class DiffusionComputation extends
  BasicComputation<LongWritable, DiffusionVertexValue, NullWritable,
    DiffusionVertexValue> {
  /**
   * Config String of the secondary load factor
   */
  public static final String SECONDARY_LOAD_FACTOR =
    "diffusion.secondaryloadfactor";
  /**
   * Config String of the edge flow scale
   */
  public static final String EDGE_FLOW_SCALE = "diffusion.edgeflowscale";
  /**
   * Config String of the number of clusters to create
   */
  public static final String NUMBER_OF_CLUSTERS = "diffusion.cluster.num";
  /**
   * Config String of total number of iterations
   */
  public static final String NUMBER_OF_ITERATIONS = "diffusion.iterations";
  /**
   * Default secondary load factor
   */
  public static final double DEFAULT_SECONDARY_LOAD_FACTOR = 10;
  /**
   * Default edge flow scale
   */
  public static final double DEFAULT_EDGE_FLOW_SCALE = 0.5;
  /**
   * Default number of clusters
   */
  public static final int DEFAULT_NUMBER_OF_CLUSTERS = 2;
  /**
   * Default number of iterations
   */
  public static final int DEFAULT_NUMBER_OF_ITERATIONS = 50;
  /**
   * Total number of clusters
   */
  private int k;
  /**
   * edge flow scale
   */
  private double edgeFlowScale;
  /**
   * secondary load factor
   */
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

  /**
   * Method to calculate the new secondary load vector
   *
   * @param vertex           actual vertex
   * @param secondaryLoads   secondary loads of all neighbor vertices
   * @param neighborClusters clusters of all neighbor vertices
   */
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

  /**
   * Method to calculate the new primary load vector
   *
   * @param vertex       actual vertex
   * @param primaryLoads primary loads of all neighbor vertices
   */
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
      System.out.println(vertex.getValue().getPrimaryLoad().size());
    }
  }

  /**
   * Method to determine new cluster (highest color weight)
   *
   * @param vertex actual vertex
   */
  private void determineNewCluster(
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

  /**
   * {@inheritDoc}
   */
  @Override
  public void compute(
    Vertex<LongWritable, DiffusionVertexValue, NullWritable> vertex,
    Iterable<DiffusionVertexValue> messages) throws IOException {
    if (getSuperstep() == 0) {
      setStartCluster(vertex);
      setStartLoad(vertex);
      sendMessageToAllEdges(vertex, vertex.getValue());
    } else {
      List<Integer> neighborClusters = new ArrayList<>();
      List<List<Double>> primaryLoadMessages = new ArrayList<>();
      List<List<Double>> secondaryLoadMessages = new ArrayList<>();
      for (DiffusionVertexValue neighborValue : messages) {
        neighborClusters.add(neighborValue.getCurrentCluster().get());
        primaryLoadMessages.add(neighborValue.getPrimaryLoad());
        secondaryLoadMessages.add(neighborValue.getSecondaryLoad());
      }
      calculateNewSecondaryLoad(vertex, secondaryLoadMessages,
        neighborClusters);
      calculateNewPrimaryLoad(vertex, primaryLoadMessages);
      if (getSuperstep() % 10 == 0) {
        determineNewCluster(vertex);
      }
      sendMessageToAllEdges(vertex, vertex.getValue());
    }
    vertex.voteToHalt();
  }
}
