package sgc.graph;

public class StringDimension implements Dimension {

	private String value;

	public StringDimension(String value){
		this.value = value;
	}

	@Override
	public Dimension getAggregate() {
		return new StringDimension("*");
	}

	public String toString(){
		return this.value;
	}

}
