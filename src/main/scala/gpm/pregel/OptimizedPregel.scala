package gpm.pregel

import gpm.data.Message
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.util.LongAccumulator

import scala.collection.mutable
import scala.reflect.{ClassTag, classTag}

class OptimizedPregel(active : mutable.Map[Byte,Long]) {
    /**
      * An optimized implementation of Pregel that considers bidirectional messaging such as the one used in
      * Dual Simulation, Strong Simulation, Strict Simulation and BDSim
      *
      * */
    var activeVertices = active
    def apply[VD: ClassTag, ED: ClassTag, A: ClassTag]
    (graph: Graph[VD, ED],
     initialMsg: Array[Message[A]],
     maxIterations: Byte = Byte.MaxValue,
     activeDirection: EdgeDirection = EdgeDirection.Either)
    (vprog: (Byte, LongAccumulator) => (VertexId, VD, Array[Message[A]]) => VD,
     sendMsg: (Byte) => (EdgeContext[VD, ED, Array[Message[A]]]) => Unit,
     mergeMsg: (Array[Message[A]], Array[Message[A]]) => Array[Message[A]]): Graph[VD, ED] = {
        require(maxIterations > 0, s"Maximum number of iterations must be greater than 0," + s" but got ${maxIterations}")

        val accumulator = SparkContext.getOrCreate().longAccumulator("nbchanges")
        var superstep = activeVertices.size.toByte
        var g = graph.mapVertices((vid, vdata) => vprog(superstep, accumulator)(vid, vdata, initialMsg))
        // compute the messages
        var messages = g.aggregateMessages(sendMsg(superstep), mergeMsg)
        var activeMessages = messages.count()
        activeVertices += superstep -> activeMessages

        var nbchanges = accumulator.value
        accumulator.reset()

        val method = g.getClass.getMethod("aggregateMessagesWithActiveSet",
            classOf[EdgeContext[VD, ED, Array[Message[A]]] => Unit],
            classOf[(Array[Message[A]], Array[Message[A]]) => Array[Message[A]]],
            classOf[TripletFields],
            classOf[Option[(VertexRDD[Array[Message[A]]], EdgeDirection)]],
            classOf[ClassTag[Array[Message[A]]]]
        )
        method.setAccessible(true)

        // Loop
        var prevG: Graph[VD, ED] = null
        var i = 0
        while (nbchanges > 0 && i < maxIterations) {
            superstep = (superstep + 1).toByte
            // Receive the messages and update the vertices.
            prevG = g
            g = g.joinVertices(messages)(vprog(superstep, accumulator))
            val oldMessages = messages

            // Send new messages, skipping edges where neither side received a message. We must cache
            // messages so it can be materialized on the next line, allowing us to uncache the previous
            // iteration.

            messages = method.invoke(g, sendMsg(superstep), mergeMsg, TripletFields.All,
                Some((oldMessages, activeDirection)), classTag[Array[Message[A]]])
              .asInstanceOf[VertexRDD[Array[Message[A]]]].persist()
            // The call to count() materializes `messages` and the vertices of `g`. This hides oldMessages
            // (depended on by the vertices of g) and the vertices of prevG (depended on by oldMessages
            // and the vertices of g).
            activeMessages = messages.count()

            // check if there was changes
            nbchanges = accumulator.value
            accumulator.reset()
            if (nbchanges > 0) {
                activeVertices += superstep -> activeMessages
            }
            // count the iteration
            i += 1

        }
        g
    } // end of apply
}
