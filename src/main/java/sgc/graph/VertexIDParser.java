package sgc.graph;

/**
 *
 * Maps String input key into ArrayVertexID<String>[] (length 1 for vertex, length 2 for edges)
 * @author Benoit Denis
 *
 */
public class VertexIDParser {

  /**
   * Parses a vertex or an edge ID
   *
   * input Input to be parsed
   * return A representation of the VertexID or the EdgeID (length 1 for Vertex, 2 for Edges)
   *
   **/
  public static ArrayVertexID[] parseID(String input){
    String[]explode = input.split("ϱ");
    ArrayVertexID[] toReturn = null;

    if(explode.length == 1){ //this is a vertex
      toReturn = new ArrayVertexID[1];
      toReturn[0] = parseVertex(explode[0],"₠");
    }
    else{ //it is an edge
      toReturn = new ArrayVertexID[2];
      String first;
      String second;

      //Makes the edge sorted (in networks, edges are undirected)
      if(explode[0].compareTo(explode[1]) < 0){
        first = explode[0];
        second = explode[1];
      }
      else{
        first = explode[1];
        second = explode[0];
      }

      toReturn[0] = parseVertex(first,"₠");
      toReturn[1] = parseVertex(second,"₠");
    }

    return toReturn;
  }

  private static ArrayVertexID parseVertex(String input, String vertexSeparator){

    String[]explode = input.split(vertexSeparator);
    StringDimension[]dimensions = new StringDimension[explode.length];

    /*
    if(explode.length != this.dimension){
      System.out.println("Expected number of dimensions : " + this.dimension);
      System.out.println("Input key number of dimensions : " + explode.length);
      System.out.println("Input string : " + input);
      throw new IOException("Wrong number of dimensions in input key");
    }*/

    for(int i = 0; i<dimensions.length; i++){
      dimensions[i] = new StringDimension(explode[i]);
    }
    return new ArrayVertexID(dimensions,dimensions.length);

  }

}
