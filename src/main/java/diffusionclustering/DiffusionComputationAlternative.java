package diffusionclustering;

import org.apache.giraph.comm.WorkerClientRequestProcessor;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.GraphState;
import org.apache.giraph.graph.GraphTaskManager;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.worker.WorkerContext;
import org.apache.giraph.worker.WorkerGlobalCommUsage;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Niklas Teichmann (teichmann@informatik.uni-leipzig.de)
 * @author Stefan Faulhaber (faulhaber@informatik.uni-leipzig.de)
 * @author Kevin Gomez      (gomez@studserv.uni-leipzig.de)
 *
 * Todo: Modularity for clustering
 * Todo: Didic vs LabelPropagation
 * Todo: Edgeflowscale with minor changings
 * Todo: Edgecut and visualization
 */
public class DiffusionComputationAlternative extends
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
  public static final double DEFAULT_EDGE_FLOW_SCALE = 1.0;
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
   * Setting start cluster (hashing)
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
   */
  private void calculateNewSecondaryLoad(
    Vertex<LongWritable, DiffusionVertexValue, NullWritable> vertex,
    List<List<Double>> secondaryLoads) {
    for (int i = 0; i < vertex.getValue().getSecondaryLoad().size(); i++) {
      double vertexLoad = vertex.getValue().getSecondaryLoad().get(i);
      for (List<Double> secondaryLoad : secondaryLoads) {
        double messageLoad = secondaryLoad.get(i);
        vertexLoad += messageLoad;
      }
      vertex.getValue().getSecondaryLoad().set(i, vertexLoad);
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
      for (List<Double> primaryLoad : primaryLoads) {
        double messageLoad = primaryLoad.get(i);
        vertexLoad += messageLoad;
      }
      vertexLoad += vertex.getValue().getSecondaryLoad().get(i);
      vertex.getValue().getPrimaryLoad().set(i, vertexLoad);
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

  private Iterable<Double> computeSecondaryLoadMessage(
    Vertex<LongWritable, DiffusionVertexValue, NullWritable> vertex) {
    List<Double> secondary = new ArrayList<>();
    for(int i=0;i<vertex.getValue().getSecondaryLoad().size();i++){
      Double secLoad = vertex.getValue().getSecondaryLoad().get(i);
      double vertexModifier = 1.0;
      if (i == vertex.getValue().getCurrentCluster().get()) {
        vertexModifier = secondaryLoadFactor;
      }
      if(vertex.getNumEdges()>0) {
        Double outMessageSum = (secLoad * edgeFlowScale) / vertexModifier;
        secondary.add(outMessageSum / vertex.getNumEdges());
        vertex.getValue().getSecondaryLoad().set(i, secLoad-outMessageSum);
      }
    }
    return secondary;
  }

  private Iterable<Double> computePrimaryLoadMessage(
    Vertex<LongWritable, DiffusionVertexValue, NullWritable> vertex) {
    List<Double> primary = new ArrayList<>();
    for(int i=0;i<vertex.getValue().getPrimaryLoad().size();i++){
      Double primLoad = vertex.getValue().getPrimaryLoad().get(i);
      if(vertex.getNumEdges()>0){
        Double outMessageSum = primLoad * edgeFlowScale;
        primary.add(outMessageSum / vertex.getNumEdges());
        vertex.getValue().getPrimaryLoad().set(i, primLoad-outMessageSum);
      }
    }
    return primary;
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
    } else {
      List<List<Double>> primaryLoadMessages = new ArrayList<>();
      List<List<Double>> secondaryLoadMessages = new ArrayList<>();
      for (DiffusionVertexValue neighborValue : messages) {
        primaryLoadMessages.add(neighborValue.getPrimaryLoad());
        secondaryLoadMessages.add(neighborValue.getSecondaryLoad());
      }
      calculateNewSecondaryLoad(vertex, secondaryLoadMessages);
      calculateNewPrimaryLoad(vertex, primaryLoadMessages);
      if (getSuperstep() % 10 == 0) {
        determineNewCluster(vertex);
      }
    }
    System.out.println(getSuperstep() + " step");
    System.out.println(vertex.getId());
    System.out.println(vertex.getValue().getPrimaryLoad() + " <<<<");
    System.out.println(vertex.getValue().getSecondaryLoad() + " <<<<");
    Iterable<Double> primMessage = computePrimaryLoadMessage(vertex);
    Iterable<Double> secMessage = computeSecondaryLoadMessage(vertex);
    System.out.println(primMessage + " <<<<");
    System.out.println(secMessage + " <<<<");
    DiffusionVertexValue message = new DiffusionVertexValue();
    message.setCurrentCluster(vertex.getValue().getCurrentCluster());
    message.setSecondaryLoad(secMessage);
    message.setPrimaryLoad(primMessage);
    sendMessageToAllEdges(vertex, message);
    vertex.voteToHalt();
  }
}
