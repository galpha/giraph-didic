package diffusionclustering;

import com.google.common.collect.Lists;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DiffusionVertexValue implements Writable {
  private List<Double> primaryLoad;
  private List<Double> secondaryLoad;
  private int currentCluster;

  public DiffusionVertexValue() {
    primaryLoad = new ArrayList<>();
    secondaryLoad = new ArrayList<>();
    currentCluster = Integer.MAX_VALUE;
  }

  public void setPrimaryLoad(Iterable<Double> primaryLoad) {
    this.primaryLoad = Lists.newArrayList(primaryLoad);
  }

  public List<Double> getPrimaryLoad() {
    return primaryLoad;
  }

  public void setSecondaryLoad(Iterable<Double> secondaryLoad) {
    this.secondaryLoad = Lists.newArrayList(secondaryLoad);
  }

  public List<Double> getSecondaryLoad() {
    return secondaryLoad;
  }

  public void setCurrentCluster(IntWritable currentCluster) {
    this.currentCluster = currentCluster.get();
  }

  public IntWritable getCurrentCluster() {
    return new IntWritable(this.currentCluster);
  }

  public void initList(int size) {
    this.primaryLoad = Lists.newArrayListWithCapacity(size);
    this.secondaryLoad = Lists.newArrayListWithCapacity(size);
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeInt(this.currentCluster);
    if (primaryLoad == null || primaryLoad.isEmpty()) {
      dataOutput.writeInt(0);
    } else {
      dataOutput.writeInt(primaryLoad.size());
      for (Double load : primaryLoad) {
        dataOutput.writeDouble(load);
      }
      for (Double load : this.secondaryLoad) {
        dataOutput.writeDouble(load);
      }
    }
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    this.currentCluster = dataInput.readInt();
    final int loadSize = dataInput.readInt();
    if (loadSize > 0) {
      initList(loadSize);
    }
    for (int i = 0; i < loadSize; i++) {
      primaryLoad.add(dataInput.readDouble());
    }
    for (int i = 0; i < loadSize; i++) {
      secondaryLoad.add(dataInput.readDouble());
    }
  }
}
