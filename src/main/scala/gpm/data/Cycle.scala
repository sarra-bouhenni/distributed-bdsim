package gpm.data

import org.apache.spark.graphx._

import scala.collection.mutable.Seq

case class Cycle(edges: Seq[Edge[Boolean]], orderedNodes: Seq[VertexId]) extends Comparable[Cycle]{

  private val sortedEdges: Seq[Edge[Boolean]] = edges.sortBy(edge => (edge.srcId, edge.dstId))
  // Step 1 - proper signature for `canEqual`
  // Step 2 - compare `a` to the current class
  override def canEqual(a: Any): Boolean = a.isInstanceOf[Cycle]

  // Step 3 - proper signature for `equals`
  // Steps 4 thru 7 - implement a `match` expression
  override def equals(that: Any): Boolean = {
    that match {
      case that: Cycle => {
        that.canEqual(this) &&
          that.edges.size == this.edges.size &&
          that.sortedEdges == this.sortedEdges
      }
      case _ => false
    }
  }

  // Step 8 - implement a corresponding hashCode method
  override def hashCode: Int = {
    var result =  41
    for(e <- this.edges){
      result = result + (e.srcId * e.dstId / (e.srcId+1)).toInt
    }
    result
  }

  override def compareTo(o: Cycle): Int = {
    if(this == o) 0 // if the cycles are equal than they return zero
    // if they are not equal, compare their lengths
    else if(this.edges.size >= o.edges.size) 1
    else -1
  }

  override def toString: String = edges.mkString("(", " - ", ")")
}
