package gpm.models

import gpm.data.Message._
import gpm.data.{Cycle, MatchedNode, Message, Token}
import gpm.pregel.OptimizedPregel
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx._
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object BDualSim {

    val IN_DIR: Byte = 5
    val OUT_DIR: Byte = 6

    type Data1 = (Boolean, Map[VertexId, (Set[VertexId], Set[VertexId])], Map[VertexId, Set[VertexId]], Map[VertexId, Set[VertexId]], Message[Set[(VertexId, VertexId)]])

    type Data[T] = (Boolean, Map[VertexId, (Set[VertexId], Set[VertexId])], Map[VertexId, Set[VertexId]], Map[VertexId, Set[VertexId]], Map[Byte, (Vector[MatchedNode], Byte)], Array[Message[T]])

    type LocalCycles = Map[Byte, (Vector[MatchedNode], Byte)]

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Test BDSim").setMaster("local[*]")
        val sc = new SparkContext(conf)
        sc.setLogLevel("ERROR")

        val g = Util.graph(sc)
        val p = Util.pattern(sc)
        val k: Byte = 1

        val shortestCycles = ShortestCycles(p)

        /** measuring running time */
        println("started evaluating BDSIM ...")
        val start = System.currentTimeMillis()
        val (matchgraph, msgsCount, steps, activeVertices) = BDualSim(sc, g, p)(shortestCycles, k)
        val end = System.currentTimeMillis()
        val runningTime = end - start
        println("finished evaluating BDSIM ...")

        println("k = " + k)
        println("set of shortest cycles = " + shortestCycles.mkString("[", ", ", "]"))
        println("num vertices  = " + matchgraph.numVertices)
        println("num edges = " + matchgraph.numEdges)
        println("num exchanged messages = " + msgsCount)
        println("num super-steps = " + steps)
        println("num active vertices by super-step = " + Util.toSortedString(activeVertices))
        println("running time " + runningTime + " ms")

        sc.stop()
    }

    def apply(sc: SparkContext, graph: Graph[Int, Int], pattern: Graph[Int, Int])
             (shortestCycles: mutable.Set[Cycle], k: Byte) = {

        val cycles = shortestCycles.zipWithIndex.map { case (cycle, id) => (cycle, {
            val seeds = mutable.Map.empty[Byte, (VertexId, Byte)]
            val zipped = cycle.orderedNodes.zip(cycle.edges).zipWithIndex.map { case ((a, b), i) => (a, b, i.toByte) }
            for ((node, e, i) <- zipped)
                if (e.srcId == node) seeds(i) = (node, OUT_DIR)
                else if (e.dstId == node) seeds(i) = (node, IN_DIR)
            seeds
        }.toMap, id.toByte)
        }.toSet

        val cyclesBroadcast = sc.broadcast(cycles)
        val accumulator = sc.longAccumulator("BDmsgsCount")
        accumulator.reset()

        val (dualMatchGraph, dMsgsCount, dSteps, dActiveMessages) = DSim(sc, graph, pattern)
        val (bdsim, activeVertices) = evaluate(dualMatchGraph, cyclesBroadcast, dActiveMessages, k, accumulator)

        val msgs = accumulator.value + dMsgsCount
        val steps = activeVertices.size + 1
        (bdsim, msgs, steps, activeVertices)
    }

    /** Evaluate dual simulation on the input pattern and data graphs
      * the dual match graph contains node id, match flag and match set,
      * parents match sets and children match sets* */

    def evaluate(dualMatchGraph: Graph[Data1, Int],
                 cycles: Broadcast[Set[(Cycle, Map[Byte, (VertexId, Byte)], Byte)]],
                 dActiveVertices: mutable.Map[Byte, Long],
                 k: Byte,
                 accumulator: LongAccumulator): (Graph[Data[Set[(VertexId, VertexId)]], Int], mutable.Map[Byte, VertexId]) = {
        /**
          * now as a data node, I have a list of cycles where I belong,
          * I need to check the length of every cycle and remove myself from those exceeding a certain limit
          * A cycle contains the list of edges, ID of its initiator and its ID
          */
        val pregelAPI = new OptimizedPregel(dActiveVertices)

        val answer: Graph[Data[Set[(VertexId, VertexId)]], Int] = if (cycles.value.nonEmpty) {
            val initMessage1 = Array(Message.init[Token])
            val initMessage2 = Array(Message.init[Set[(VertexId, VertexId)]])

            /** Second step, we assign to each data vertex its corresponding cycles * */
            val a = dualMatchGraph.mapVertices((_, vData) => (vData._1, vData._2, vData._3, vData._4, assignCycles(cycles, vData._2.keySet), Array.empty[Message[Token]]))

            /** Third step, we execute token passing * */
            val maxIterations = (k * cycles.value.map(_._1.edges.size).max).toByte
            val b = pregelAPI.apply(a, initMessage1, maxIterations)(vertexProgTokenPassing(k, accumulator), sendTokenMessage, mergeMsgs)
              .mapVertices((_, vData) => (vData._1, vData._2, vData._3, vData._4, vData._5, Array.empty[Message[Set[(VertexId, VertexId)]]]))


            /** Final step, we remove non valid matches resulting from token passing * */
            val c = pregelAPI.apply(b, initMessage2, maxIterations = 100)(vertexProgFiltering(accumulator), sendRemovalMessage, mergeMsgs)
            val filteredG = c.subgraph(vpred = (_, v) => v._1)

            filteredG
        }
        else dualMatchGraph
          .mapVertices((_, vData) => (vData._1, vData._2, vData._3, vData._4, Map.empty, Array.empty))

        (answer, pregelAPI.activeVertices)
    }


    def assignCycles(cycles: Broadcast[Set[(Cycle, Map[Byte, (VertexId, Byte)], Byte)]],
                     matchSet: Set[VertexId]): LocalCycles = {
        // my local cycle variable => cycle ID -> its nodes that are present in local match set with their positions, its size
        // Map[Byte, (VertexId, Byte)] means: position -> vertexId and direction in cycle
        var myLocalCycles = Map[Byte, (Vector[MatchedNode], Byte)]() // cycle id -> liste of matched nodes, cycle size, nb token
        for ((cycle, seeds, cid) <- cycles.value) {
            val edges = cycle.edges
            var matchedNodes = Set[VertexId]()
            edges.foreach(edge => {
                if (matchSet.contains(edge.srcId)) {
                    matchedNodes += edge.srcId
                }
                if (matchSet.contains(edge.dstId)) {
                    matchedNodes += edge.dstId
                }
            })
            if (matchedNodes.nonEmpty) {
                /** Among the positions of cycle members, I care about my position, my direction and what is next node* */
                val mySeeds = seeds.filter(matchedNodes contains _._2._1).map { case (pos, (mnode, dir)) =>
                    val nextPos = (pos + 1) % edges.size
                    val next = seeds(nextPos.toByte)
                    MatchedNode(matchedNode = mnode, position = pos, direction = dir, nextMatchedNode = next._1, tokens = false)
                }.toVector

                myLocalCycles = myLocalCycles + (cid -> (mySeeds, edges.size.toByte))
            }
        }
        myLocalCycles
    }

    def vertexProgTokenPassing(k: Byte,
                               accumulator: LongAccumulator)
                              (steps: Byte,
                   pregelAcc: LongAccumulator)(
                    id: VertexId,
                    vData: Data[Token],
                    msgs: Array[Message[Token]]): Data[Token] = {
        var myLocalCycles = vData._5
        var dataToSend = Array.empty[Message[Token]]
        accumulator.add(msgs.length)
        if (msgs.exists(_.isInit)) {
            /**
              * A token contains: ID of its data vertex, ID of its initiator in the query, Cycle ID, Cycle edges, Path
              * a message contains: Destination ID, list of tokens
              * If I have a cycle, I trigger the token passing procedure
              * I set a token containing, the cycle id , my id, an index
              */
            dataToSend ++=
              myLocalCycles.flatMap { case (cuuid, (matchedNodes, clen)) => {
                  matchedNodes.flatMap(cyclematchedNode => {
                      if (cyclematchedNode.direction == OUT_DIR)
                          vData._4.flatMap(cmatch => if (cmatch._2 contains cyclematchedNode.nextMatchedNode) {
                              //  val element = (cuuid, cyclematchedNode, clen, cmatch)
                              val newToken = Token(id, cuuid, cyclematchedNode.nextMatchedNode, expiring = (clen * k).toByte)
                              val msg = Message(flag = TOKEN_MESSAGE, data = Some(newToken), dst = cmatch._1, step = steps)
                              Vector(msg)
                          }
                          else Vector())
                      else // direction == IN_DIR
                          vData._3.flatMap(pmatch => if (pmatch._2 contains cyclematchedNode.nextMatchedNode) {
                              //val element = (cuuid, cyclematchedNode, clen, pmatch)
                              val newToken = Token(id, cuuid, cyclematchedNode.nextMatchedNode, expiring = (clen * k).toByte)
                              val msg = Message(flag = TOKEN_MESSAGE, data = Some(newToken), dst = pmatch._1, step = steps)
                              Vector(msg)
                          }
                          else Vector[Message[Token]]())
                  })
              }
              }
        }
        val tokens = msgs.filter(t => t.isToken).map(_.data.get.decrement())
        // filter received tokens to the ones i have initiated
        // define specific token counter for each matched node
        tokens.filter(token => token.initiator == id)
          .foreach(t => if (myLocalCycles.contains(t.cycleId)) {
              val (seeds, nbEdges) = myLocalCycles(t.cycleId)
              val specificMatchedNode = seeds.find(m => m.matchedNode == t.matchedNode)
              specificMatchedNode match {
                  case Some(matchedNode) => {
                      val newSeeds = seeds.filterNot(m => m.matchedNode == t.matchedNode) :+ matchedNode.incrementTokenCount()
                      myLocalCycles = myLocalCycles - t.cycleId
                      myLocalCycles = myLocalCycles + (t.cycleId -> (newSeeds, nbEdges))
                  }
                  case None => {}
              }
          })

        // other nodes tokens
        dataToSend ++=
          tokens
            .flatMap(token => if (token.expiring > 0 && token.initiator != id) {
                myLocalCycles
                  .flatMap { case (cid, (matchedNodes, clen)) =>
                      if (cid == token.cycleId)
                          matchedNodes.flatMap(matchedNode => {
                              if (matchedNode.matchedNode == token.matchedNode) {
                                  val newToken = token.move(matchedNode.nextMatchedNode)
                                  if (matchedNode.direction == OUT_DIR)
                                      vData._4.flatMap(cmatch => {
                                          // val element =(cid, matchedNode, cmatch)
                                          if (cmatch._2.contains(matchedNode.nextMatchedNode))
                                              Vector(Message(flag = TOKEN_MESSAGE, data = Some(newToken), dst = cmatch._1, step = steps))
                                          else Vector[Message[Token]]()
                                      })
                                  else // if(matchedNode.direction == IN_DIR)
                                      vData._3.flatMap(pmatch => {
                                          //val element = (cid, matchedNode, pmatch)
                                          if (pmatch._2.contains(matchedNode.nextMatchedNode))
                                              Vector(Message(flag = TOKEN_MESSAGE, data = Some(newToken), dst = pmatch._1, step = steps))
                                          else Vector[Message[Token]]()
                                      })
                              }
                              else None
                          })
                      else None
                  }
            }
            else None)

        if (dataToSend.nonEmpty) pregelAcc.add(1)

        (vData._1, vData._2, vData._3, vData._4, myLocalCycles, dataToSend)
    }

    def vertexProgFiltering(accumulator: LongAccumulator)
                           (steps: Byte,
                            pregelAcc: LongAccumulator)(
                             id: VertexId,
                             vData: Data[Set[(VertexId, VertexId)]],
                             receivedData: Array[Message[Set[(VertexId, VertexId)]]]): Data[Set[(VertexId, VertexId)]] = {
        // reevaluate dual simulation constraints
        // I send msg to my parents, if there is an update where I should remove a tuple from my matchSet
        accumulator.add(receivedData.length)
        var matchFlag = vData._1
        if (matchFlag) {
            var constraints = vData._2
            var pmatchSet = vData._3
            var cmatchSet = vData._4

            val reevaluateMatching: Boolean = receivedData.exists(msg => msg.isInit || msg.isRemoval)
            val receivedRemovedMappings: Set[(VertexId, VertexId)] = receivedData.flatMap(msg => if (msg.isRemoval) msg.data.get else None).toSet

            /**
              * if number of received tokens for a matched node in this cycle is zero !!
              * * remove this matched node from my matchedSet
              * * inform my parents and children by adding myself to removedMappings list
              */
            var removedNodes: Set[VertexId] =
                if (receivedData.exists(_.isInit))
                    vData._5.flatMap(_._2._1.filter(matchedNode => !matchedNode.tokens).map(_.matchedNode)).toSet
                else
                    Set.empty[VertexId]

            receivedRemovedMappings.foreach(mapping => {
                if (pmatchSet.contains(mapping._1)) {
                    val updatedValue = pmatchSet(mapping._1) - mapping._2 // filter out removed pairs from pmatchSet
                    pmatchSet = pmatchSet - mapping._1
                    if (updatedValue.nonEmpty) pmatchSet = pmatchSet + (mapping._1 -> updatedValue)
                }
                if (cmatchSet.contains(mapping._1)) {
                    val updatedValue = cmatchSet(mapping._1) - mapping._2 // filter out removed pairs from pmatchSet
                    cmatchSet = cmatchSet - mapping._1
                    if (updatedValue.nonEmpty) cmatchSet = cmatchSet + (mapping._1 -> updatedValue)
                }
            })
            if (reevaluateMatching) {
                var changed = true
                while (changed) {
                    changed = false
                    for ((qnode, (pcons, ccons)) <- constraints) {
                        val childcandidates = cmatchSet.values.flatten.toSet
                        if (!ccons.subsetOf(childcandidates)) {
                            // a removal message should be sent
                            // remove this query from matched pairs pairs
                            removedNodes += qnode
                            changed = true
                        }
                        val parentcandidates = pmatchSet.values.flatten.toSet
                        if (!pcons.subsetOf(parentcandidates)) {
                            // a removal message should be sent
                            // remove this query from matched pairs pairs
                            removedNodes += qnode
                            changed = true
                        }
                    }
                    constraints = constraints -- removedNodes
                }
                matchFlag = constraints.nonEmpty
                // inform my parents and children by adding myself to removedMappings list
            }
            if (removedNodes.nonEmpty) pregelAcc.add(1)
            val msg = if (removedNodes.nonEmpty)
                Array(Message(REMOVAL_MESSAGE, Some(removedNodes.map((id, _))), 0, steps))
            else Array.empty[Message[Set[(VertexId, VertexId)]]]

            (matchFlag, constraints, pmatchSet, cmatchSet, vData._5, msg)
        }
        else
            (vData._1, vData._2, vData._3, vData._4, vData._5, Array.empty[Message[Set[(VertexId, VertexId)]]])
    }

    def sendTokenMessage(steps: Byte)(triplet: EdgeContext[Data[Token], Int, Array[Message[Token]]]): Unit = {
        val srcMsgs = triplet.srcAttr._6.filter(m => m.dst == triplet.dstId)
        val dstMsgs = triplet.dstAttr._6.filter(m => m.dst == triplet.srcId) // null pointer exception

        if (srcMsgs.nonEmpty && srcMsgs.head.step == steps)
            triplet.sendToDst(srcMsgs)
        if (dstMsgs.nonEmpty && dstMsgs.head.step == steps)
            triplet.sendToSrc(dstMsgs)
    }

    def sendRemovalMessage(steps: Byte)(triplet: EdgeContext[Data[Set[(VertexId, VertexId)]], Int, Array[Message[Set[(VertexId, VertexId)]]]]): Unit = {
        val srcMsgs = triplet.srcAttr._6
        val dstMsgs = triplet.dstAttr._6

        if (triplet.dstAttr._1 &&
          srcMsgs.nonEmpty &&
          srcMsgs.head.isRemoval &&
          srcMsgs.head.step == steps)
            triplet.sendToDst(srcMsgs)
        if (triplet.srcAttr._1 &&
          dstMsgs.nonEmpty &&
          dstMsgs.head.isRemoval &&
          dstMsgs.head.step == steps)
            triplet.sendToSrc(dstMsgs)
    }

    def mergeMsgs[T](a: Array[Message[T]], b: Array[Message[T]]): Array[Message[T]] = (a ++ b).distinct

}
