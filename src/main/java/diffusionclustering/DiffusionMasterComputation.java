package diffusionclustering;

import org.apache.giraph.master.DefaultMasterCompute;

/**
 * Master Computation for {@link DiffusionComputation}.
 * <p/>
 * Halts the computation after a given number if iterations.
 *
 * @author Niklas Teichmann (teichmann@informatik.uni-leipzig.de)
 * @author Stefan Faulhaber (faulhaber@informatik.uni-leipzig.de)
 * @author Kevin Gomez (gomez@studserv.uni-leipzig.de)
 */
public class DiffusionMasterComputation extends DefaultMasterCompute {
  /**
   * {@inheritDoc}
   */
  @Override
  public void compute() {
    int iterations = getConf().getInt(DiffusionComputation.NUMBER_OF_ITERATIONS,
      DiffusionComputation.DEFAULT_NUMBER_OF_ITERATIONS);
    if (getSuperstep() == iterations) {
      haltComputation();
    }
  }
}
