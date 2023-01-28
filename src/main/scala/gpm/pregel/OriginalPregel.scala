package gpm.pregel

import org.apache.spark.graphx._

import scala.collection.mutable
import scala.reflect._

class OriginalPregel(active : mutable.Map[Byte,Long]) {
  /**
    * This version of Pregel is similar to that of GraphX,
    * the only difference is in performance metrics that are reported inside and returned at the end.
    */
  var activeVertices = active
  var stepCount : Byte= active.size.toByte

  def apply[VD: ClassTag, ED: ClassTag, A: ClassTag]
  (graph: Graph[VD, ED],
   initialMsg: A,
   maxIterations: Byte = Byte.MaxValue,
   activeDirection: EdgeDirection = EdgeDirection.Either)
  (vprog: (VertexId, VD, A) => VD,
   sendMsg: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
   mergeMsg: (A, A) => A)
  : Graph[VD, ED] =
  {
    require(maxIterations > 0, s"Maximum number of iterations must be greater than 0," + s" but got ${maxIterations}")

    val method = GraphXUtils.getClass.getMethod("mapReduceTriplets",
      classOf[Graph[VD, ED]],
      classOf[EdgeTriplet[VD, ED] => Iterator[(VertexId, A)]],
      classOf[(A, A) => A],
      classOf[Option[(VertexRDD[_], EdgeDirection)]],
      classOf[ClassTag[VD]],
      classOf[ClassTag[ED]],
      classOf[ClassTag[A]]
    )
    method.setAccessible(true)
    var g = graph.mapVertices((vid, vdata) => vprog(vid, vdata, initialMsg))
    // compute the messages
    var messages = method.invoke(GraphXUtils, g, sendMsg, mergeMsg, None, classTag[VD], classTag[ED], classTag[A]).asInstanceOf[VertexRDD[A]]
    var activeMessages = messages.count()
    activeVertices += stepCount -> activeMessages
    stepCount = (stepCount + 1).toByte

    // Loop
    var prevG: Graph[VD, ED] = null
    var i = 0
    while (activeMessages > 0 && i < maxIterations) {
      // Receive the messages and update the vertices.
      prevG = g
      g = g.joinVertices(messages)(vprog)

      val oldMessages = messages
      // Send new messages, skipping edges where neither side received a message. We must cache
      // messages so it can be materialized on the next line, allowing us to uncache the previous
      // iteration.
      messages = method.invoke(GraphXUtils,g, sendMsg, mergeMsg,
        Some((oldMessages, activeDirection)),classTag[VD], classTag[ED], classTag[A]).asInstanceOf[VertexRDD[A]]
      // The call to count() materializes `messages` and the vertices of `g`. This hides oldMessages
      // (depended on by the vertices of g) and the vertices of prevG (depended on by oldMessages
      // and the vertices of g).
      activeMessages = messages.count()
      if(activeMessages>0){
        activeVertices += stepCount -> activeMessages
        stepCount = (stepCount + 1).toByte
      }
      oldMessages.unpersist()
      prevG.unpersistVertices()
      prevG.edges.unpersist()
      // count the iteration
      i += 1
    }
    g
  } // end of apply
}