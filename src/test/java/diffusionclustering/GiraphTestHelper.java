package diffusionclustering;

public abstract class GiraphTestHelper {
  public static String[] getDirectedGraphFrom1To0() {
    return new String[] {
      "0 1",
      "1 0 2",
      "2 1"
    };
  }

  public static String[] getAlternativeExample() {
    return new String[] {
      "0 3",
      "1 3",
      "2 3",
      "3 0 1 2"
    };
  }

  public static String[] getUndirectedGraph() {
    return new String[] {
      "0 1",
      "1 0"
    };
  }

  public static String[] getDirectedGraphFrom0To1() {
    return new String[] {
      "0 1",
      "1"
    };
  }

  public static String[] getExtremeExample() {
    return new String[] {
      "0 1 2 3 4 5 6 7 8 9 10",
      "1 0",
      "2 0",
      "3 0",
      "4 0",
      "5 0",
      "6 0",
      "7 0",
      "8 0",
      "9 0",
      "10 0"
    };
  }

  public static String[] getClusterExample() {
    return new String[] {
      "0 1 2 3",
      "1 0 2 3",
      "2 0 1 3",
      "3 0 1 2 4",
      "4 3 5 6 7",
      "5 4 6 7" ,
      "6 4 5 7",
      "7 4 5 6"
    };
  }
}
