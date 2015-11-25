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

  public static String[] getDirected4NodeGraph() {
      return new String[] {
            "0 1 2",
            "1",
            "2 1",
            "3 0"
      };
  }

}
