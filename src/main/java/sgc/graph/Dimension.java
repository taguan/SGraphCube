package main.java.sgc.graph;

/**
 **
 ** @author Benoît Denis
 **
 ** Types for dimensions.
 ** Dimensions knows themselves what are their aggregated form.
 **/
public interface Dimension {

	/**
	 ** 
	 ** @return aggregate form of this dimension
	 **/
	public Dimension getAggregate();
}
