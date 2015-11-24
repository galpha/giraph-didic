package diffusionclustering;

public abstract class GiraphTestHelper {
  public static String[] getDirectedGraphFrom1To0() {
    return new String[] {
      "0",
      "1 0"
    };
  }

  public static String[] getDirectedGraphFrom0To1() {
    return new String[] {
      "0 1",
      "1"
    };
  }
}
