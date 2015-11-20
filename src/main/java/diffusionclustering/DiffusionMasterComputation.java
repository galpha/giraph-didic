package diffusionclustering;

import org.apache.giraph.master.DefaultMasterCompute;

/**
 * Master Computation for {@link DiffusionComputation}.
 * <p/>
 * Halts the computation after a given number if iterations.
 *
 * @author Kevin Gomez (k.gomez@freenet.de)
 * @author Martin Junghanns (junghanns@informatik.uni-leipzig.de)
 */
public class DiffusionMasterComputation extends DefaultMasterCompute {
  /**
   * {@inheritDoc}
   */
  @Override
  public void compute() {
    int iterations = 10;
    if (getSuperstep() == iterations) {
      haltComputation();
    }
  }
}