package sgc.graph;

public class ArrayVertexID {

	public int nbrOfDimensions;
	private Dimension []dimensions;

	public ArrayVertexID(int nbrOfDimensions){
		this.nbrOfDimensions = nbrOfDimensions;
		this.dimensions = null;
	}

	public ArrayVertexID(Dimension []dimensions, int nbrOfDimensions){
		this(nbrOfDimensions);
		this.dimensions = dimensions;
	}

    public ArrayVertexID clone(){
        return new ArrayVertexID(this.dimensions.clone(),this.nbrOfDimensions);
    }

	public Dimension getDimension(int index){
		return dimensions[index];
	}

	public void setDimension(int index, Dimension newValue){
		dimensions[index] = newValue;
	}

	public String toString(String delimiter){
		StringBuilder strb = new StringBuilder();
		for(int i = 0; i<dimensions.length-1; i++){
			strb.append(dimensions[i].toString());
			strb.append(delimiter);
		}
		strb.append(dimensions[dimensions.length-1].toString());

		return strb.toString();
	}

}

