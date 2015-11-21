package diffusionclustering;

import com.google.common.collect.Lists;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

/**
 * Used in {@DiffusionComputatoin}
 */
public class DiffusionMessage implements Writable {
  private int currentCluster;
  private List<Double> primaryLoad;
  private List<Double> secondaryLoad;

  public void setPrimaryLoad(List<Double> primaryLoad) {
    this.primaryLoad = primaryLoad;
  }

  public void setSecondaryLoad(List<Double> secondaryLoad) {
    this.secondaryLoad = secondaryLoad;
  }

  public void setCurrentCluster(int currentCluster) {
    this.currentCluster = currentCluster;
  }

  public List<Double> getPrimaryLoad() {
    return this.primaryLoad;
  }

  public List<Double> getSecondaryLoad() {
    return this.secondaryLoad;
  }

  public int getCurrentCluster() {
    return currentCluster;
  }

  private void initLists() {
    if (primaryLoad == null) {
      this.primaryLoad = Lists.newArrayList();
      this.secondaryLoad = Lists.newArrayList();
    }
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeInt(primaryLoad.size());
    for (Double load : primaryLoad) {
      dataOutput.writeDouble(load);
    }
    for (Double load : secondaryLoad) {
      dataOutput.writeDouble(load);
    }
    dataOutput.writeInt(currentCluster);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    int loadSize = dataInput.readInt();
    if(loadSize > 0) initLists();
    for (int i = 0; i < loadSize; i++) {
      primaryLoad.add(dataInput.readDouble());
    }
    for (int i = 0; i < loadSize; i++) {
      secondaryLoad.add(dataInput.readDouble());
    }
    currentCluster = dataInput.readInt();
  }
}
