package gpm.data

import org.apache.spark.graphx.VertexId

case class Token(initiator: VertexId,
                 cycleId: Byte,
                 matchedNode: VertexId,
                 expiring: Byte) {
    def decrement(): Token = Token(initiator, cycleId, matchedNode, (expiring - 1).toByte)

    def move(nextNode: VertexId): Token = Token(initiator, cycleId, nextNode, expiring)
}
