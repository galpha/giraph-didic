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

  /**
   * Default Constructor
   */
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

  public void print(){
    System.out.println("###");
    System.out.println("PrimaryLoad:");
    System.out.println(primaryLoad.toString());
    System.out.println("SecondaryLoad:");
    System.out.println(secondaryLoad.toString());
    System.out.println("currentCluster:");
    System.out.println(currentCluster);
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
    for (int i = 0; i < loadSize; i++) {
      primaryLoad.add(dataInput.readDouble());
    }
    for (int i = 0; i < loadSize; i++) {
      secondaryLoad.add(dataInput.readDouble());
    }
    currentCluster = dataInput.readInt();
  }
}
