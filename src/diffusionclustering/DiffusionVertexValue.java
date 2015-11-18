
package diffusionclustering;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DiffusionVertexValue implements Writable {

  private List<Long> primaryLoad;
  private List<Long> secondaryLoad;
  private int currentCluster;


  /**
   * Default Constructor
   */
  public DiffusionVertexValue() {
    primaryLoad = new ArrayList<>();
    secondaryLoad = new ArrayList<>();
    currentCluster = Integer.MAX_VALUE;
  }

  public DiffusionVertexValue(int loadSize, int startingCluster) {

    primaryLoad = new ArrayList<>();
    for(int i=0;i<loadSize;i++){
      if(i!= startingCluster){
        primaryLoad.add(0L);
      }
      else{
        primaryLoad.add(1L);
      }
    }
    primaryLoad = new ArrayList<>();
    
    for(int i=0;i<loadSize;i++){
      if(i!= startingCluster){
        secondaryLoad.add(0L);
      }
      else{
        secondaryLoad.add(1L);
      }
    }

    currentCluster = startingCluster;
  }

  public void setPrimaryLoad(Iterable<Long> primaryLoad){
    this.primaryLoad = Lists.newArrayList(primaryLoad);
  }

  public Iterable<Long> getPrimaryLoad(){
    return primaryLoad;
  }

  public void setSecondaryLoad(Iterable<Long> secondaryLoad){
    this.secondaryLoad = Lists.newArrayList(secondaryLoad);
  }

  public Iterable<Long> getSecondaryLoad(){
    return secondaryLoad;
  }

  public void setCurrentCluster(IntWritable currentCluster) {
    this.currentCluster = currentCluster.get();
  }

  public IntWritable getCurrentCluster() {
    return new IntWritable(this.currentCluster);
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeInt(primaryLoad.size());
    for(Long load : primaryLoad){
      dataOutput.writeLong(load);
    }
    for(Long load : secondaryLoad){
      dataOutput.writeLong(load);
    }
    dataOutput.writeInt(currentCluster);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    int loadSize = dataInput.readInt();
    for(int i=0;i<loadSize;i++){
      primaryLoad.add(dataInput.readLong());
    }
    for(int i=0;i<loadSize;i++){
      secondaryLoad.add(dataInput.readLong());
    }
    currentCluster = dataInput.readInt();
  }
}
