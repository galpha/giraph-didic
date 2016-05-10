package diffusionclustering;

public abstract class GiraphTestHelper {
  public static String[] getDirectedGraphFrom1To0() {
    return new String[] {
      "0 1",
      "1 0 2",
      "2 1"
    };
  }


  public static String[] getDirectedGraphFrom0To1() {
    return new String[] {
      "0 1",
      "1"
    };
  }
}
