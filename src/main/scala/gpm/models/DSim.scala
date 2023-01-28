package gpm.models

import gpm.data.Message
import gpm.data.Message._
import gpm.pregel.OptimizedPregel
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx._
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable

object DSim {
    type Data[T] = (Boolean, Map[VertexId, (Set[VertexId], Set[VertexId])], Map[VertexId, Set[VertexId]], Map[VertexId, Set[VertexId]], Message[T])

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Test").setMaster("local")

        val sc = new SparkContext(conf)
        sc.setLogLevel("ERROR")
        val t1 = System.currentTimeMillis()
        val (result, msgsCount, steps, activeVertices) = DSim(sc, Util.graph(sc), Util.pattern(sc))
        val t2 = System.currentTimeMillis()
        println("DSIM number of vertices " + result.numVertices + " \t edges = " + result.numEdges)
        println(msgsCount + " messages exchanged")
        println(steps + " super-steps")
        println(Util.toString(activeVertices) + " active-vertices")
        println((t2 - t1) + " ms")
        sc.stop()
    }

    def apply(sc: SparkContext, graph: Graph[Int, Boolean], pattern: Graph[Int, Int]) = {
        val vertices = pattern.vertices.collect
        val labels = sc.broadcast(vertices)
        val parentConstraints = sc.broadcast(pattern.collectNeighborIds(EdgeDirection.In).collectAsMap())
        val childConstraints = sc.broadcast(pattern.collectNeighborIds(EdgeDirection.Out).collectAsMap())
        val accumulator: LongAccumulator = sc.longAccumulator("DmsgsCount")
        accumulator.reset()
        val (dsim, activeVertices) = evaluate(graph, labels, parentConstraints, childConstraints,accumulator)
        val msgs = accumulator.value
        val steps = activeVertices.size + 1
        (dsim, msgs, steps, activeVertices)
    }

    def vertexProgOne(accumulator: LongAccumulator)(steps: Byte, pregelAcc: LongAccumulator)(id: VertexId, vData: Data[Map[VertexId, Set[VertexId]]], receivedData: Array[Message[Map[VertexId, Set[VertexId]]]]): Data[Map[VertexId, Set[VertexId]]] = {
        //evaluate matchSet and matchFlag
        // as a vertex, I have a partial matchset
        // I should compare it with my matched vertex query neighbors candidates
        // I send msg to my parents, if there is an update where I should remove a tuple from my matchSet
        val constraints = vData._2
        var pmatchSet: Map[VertexId, Set[VertexId]] = vData._3
        var cmatchSet: Map[VertexId, Set[VertexId]] = vData._4
        var dataToSend: Message[Map[VertexId, Set[VertexId]]] = Message.empty[Map[VertexId, Set[VertexId]]]
        accumulator.add(receivedData.length)
        // treat each message flag separately
        if (vData._1) {
            val constraintsValues = constraints.values
            val parentConstraints = constraintsValues.flatMap(_._1).toSet
            val childConstraints = constraintsValues.flatMap(_._2).toSet

            receivedData.foreach(message => {
                message.flag match {
                    case DATA_TO_CHILDREN =>
                        for ((k, v) <- message.data.get)
                            if (parentConstraints.intersect(v).nonEmpty) {
                                if (pmatchSet.contains(k)) {
                                    val updatedValue = pmatchSet(k) ++ v
                                    pmatchSet = pmatchSet - k
                                    pmatchSet = pmatchSet + (k -> updatedValue)
                                }
                                else {
                                    pmatchSet = pmatchSet + (k -> v)
                                }
                            }
                    case DATA_TO_PARENTS =>
                        for ((k, v) <- message.data.get)
                            if (childConstraints.intersect(v).nonEmpty) {
                                if (cmatchSet.contains(k)) {
                                    val updatedValue = cmatchSet(k) ++ v
                                    cmatchSet = cmatchSet - k
                                    cmatchSet = cmatchSet + (k -> updatedValue)
                                }
                                else {
                                    cmatchSet = cmatchSet + (k -> v)
                                }
                            }
                    case REQUEST_MESSAGE =>
                        pregelAcc.add(1)
                        dataToSend = new Message(DATA_MESSAGE, Some(Map(id -> constraints.keySet)), 0, steps) // send the local matchSet
                }
            })
        }
        (vData._1, constraints, pmatchSet, cmatchSet, dataToSend)
    }

    def sendMessageOne(steps: Byte)(triplet: EdgeContext[Data[Map[VertexId, Set[VertexId]]], Boolean, Array[Message[Map[VertexId, Set[VertexId]]]]]): Unit = {
        // if my messageType = REQUEST_MESSAGE, i send my requests to children and parents
        val srcData = triplet.srcAttr._5
        val dstData = triplet.dstAttr._5

        if (triplet.srcAttr._1 && triplet.dstAttr._1) {
            if (srcData.flag == DATA_MESSAGE && srcData.step == steps) {
                // copy message content and change flag
                triplet.sendToDst(Array(srcData.setFlag(DATA_TO_CHILDREN)))
            }
            if (dstData.flag == DATA_MESSAGE && srcData.step == steps)
            // copy message content and change flag
                triplet.sendToSrc(Array(dstData.setFlag(DATA_TO_PARENTS)))
        }
    }

    def vertexProgTwo(accumulator: LongAccumulator)(steps: Byte, pregelAcc: LongAccumulator)(id: VertexId, vData: Data[Set[(VertexId, VertexId)]], receivedData: Array[Message[Set[(VertexId, VertexId)]]]): Data[Set[(VertexId, VertexId)]] = {
        //evaluate matchSet and matchFlag
        // as a vertex, I have a partial matchset
        // I should compare it with my matched vertex query neighbors candidates
        // I send msg to my parents, if there is an update where I should remove a tuple from my matchSet
        accumulator.add(receivedData.length)
        var matchFlag = vData._1
        var message: Message[Set[(VertexId, VertexId)]] = Message.empty
        if (matchFlag) {
            var constraints = vData._2 // the keyset is local matchSet
            var pmatchSet = vData._3
            var cmatchSet = vData._4

            var removedNodes = Set[VertexId]()

            val reevaluateMatching: Boolean = receivedData.exists(msg => msg.isInit || msg.isRemoval)

            val receivedRemovedMappings = receivedData.flatMap(msg => if (msg.isRemoval) msg.data.get else None)

            // I have the list of removed tuples
            // I filter neighbors matchSet
            if (receivedRemovedMappings.nonEmpty) {
                receivedRemovedMappings.foreach(mapping => {
                    if (pmatchSet.contains(mapping._1)) {
                        val updatedValue = pmatchSet(mapping._1) - mapping._2 // filter out removed tuples from pmatchSet
                        pmatchSet = pmatchSet - mapping._1
                        if (updatedValue.nonEmpty) pmatchSet = pmatchSet + (mapping._1 -> updatedValue)
                    }
                    if (cmatchSet.contains(mapping._1)) {
                        val updatedValue = cmatchSet(mapping._1) - mapping._2 // filter out removed tuples from pmatchSet
                        cmatchSet = cmatchSet - mapping._1
                        if (updatedValue.nonEmpty) cmatchSet = cmatchSet + (mapping._1 -> updatedValue)
                    }
                })
            }

            if (reevaluateMatching) {
                var changed = true
                while (changed) {
                    changed = false
                    for ((qnode, (pcons, ccons)) <- constraints) {
                        val childcandidates = cmatchSet.values.flatten.toSet
                        if (!ccons.subsetOf(childcandidates)) {
                            // remove this query from matched tuples
                            removedNodes += qnode
                            changed = true
                            // inform my parents and children by adding myself to removedMappings list
                        }
                        val parentcandidates = pmatchSet.values.flatten.toSet
                        if (!pcons.subsetOf(parentcandidates)) {
                            // remove this query from matched tuples
                            removedNodes += qnode
                            changed = true
                            // inform my parents and children by adding myself to removedMappings list
                        }
                    }
                    constraints = constraints -- removedNodes
                }
                matchFlag = constraints.nonEmpty
            }
            if (removedNodes.nonEmpty) {
                pregelAcc.add(1)
                message = new Message(REMOVAL_MESSAGE, Some(removedNodes.map((id, _))), 0, step = steps)
            }
            // return the new vertex data
            (matchFlag, constraints, pmatchSet, cmatchSet, message)
        }
        else
            (vData._1, vData._2, vData._3, vData._4, message)
    }

    def sendMessageTwo(steps: Byte)(triplet: EdgeContext[Data[Set[(VertexId, VertexId)]], Boolean, Array[Message[Set[(VertexId, VertexId)]]]]): Unit = {
        val srcMsg = triplet.srcAttr._5
        val dstMsg = triplet.dstAttr._5

        if (srcMsg.isRemoval &&
          triplet.dstAttr._1 &&
          srcMsg.step == steps) {
            triplet.sendToDst(Array(srcMsg))
        }
        if (dstMsg.isRemoval &&
          triplet.srcAttr._1 &&
          dstMsg.step == steps) {
            triplet.sendToSrc(Array(dstMsg))
        }
    }

    def mergeMsgs[T](a: Array[Message[T]], b: Array[Message[T]]): Array[Message[T]] = (a ++ b).distinct


    def evaluate(graph: Graph[Int, Boolean], vertices: Broadcast[Array[(VertexId, Int)]],
                 parentConstraints: Broadcast[scala.collection.Map[VertexId, Array[VertexId]]],
                 childConstraints: Broadcast[scala.collection.Map[VertexId, Array[VertexId]]],
                 accumulator: LongAccumulator): (Graph[Data[Set[(VertexId, VertexId)]], Boolean], mutable.Map[Byte, VertexId]) = {

        val requestMessage = Array(Message.request[Map[VertexId, Set[VertexId]]])
        val initMessage = Array(Message.init[Set[(VertexId, VertexId)]])

        val a = graph.mapVertices((_, label) => {
            val matchSet = vertices.value.filter(v => v._2 == label).map(_._1)
            val matchFlag = matchSet.nonEmpty
            (matchFlag, // match flag
              matchSet.map(u => u -> (parentConstraints.value(u).toSet, childConstraints.value(u).toSet)).toMap, // match set
              Map.empty[VertexId, Set[VertexId]], // parents match sets
              Map.empty[VertexId, Set[VertexId]], // children match sets
              Message.empty[Map[VertexId, Set[VertexId]]] // prepared message
            )
        }).subgraph(vpred = (id, attr) => attr._1)


        val pregelAPI: OptimizedPregel = new OptimizedPregel(mutable.Map.empty[Byte, VertexId])

        val b = pregelAPI.apply(a, requestMessage, 5)(vertexProgOne(accumulator), sendMessageOne, mergeMsgs)
          .mapVertices((_, vData) => (vData._1, vData._2, vData._3, vData._4, Message.empty[Set[(VertexId, VertexId)]]))

        val c = pregelAPI.apply(b, initMessage, 100)(vertexProgTwo(accumulator), sendMessageTwo, mergeMsgs)

        val d = c.subgraph(vpred = (_, v) => v._1, epred = t => t.srcAttr._4.contains(t.dstId) || t.dstAttr._3.contains(t.srcId))

        // println("Dual match graph V = "+d.numVertices+" E = "+d.numEdges)
        (d, pregelAPI.activeVertices)
    }

}
