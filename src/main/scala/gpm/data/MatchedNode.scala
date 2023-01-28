package gpm.data

import org.apache.spark.graphx.VertexId

case class MatchedNode(matchedNode: VertexId,
                       position: Byte,
                       direction: Byte,
                       nextMatchedNode: VertexId,
                       var tokens: Boolean = false) extends Serializable {
    def incrementTokenCount(): MatchedNode = {
        this.tokens = true
        this
    }

}

