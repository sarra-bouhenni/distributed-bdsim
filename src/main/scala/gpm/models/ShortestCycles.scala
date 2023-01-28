package gpm.models

import gpm.data.Cycle
import org.apache.spark.graphx._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.Seq
import scala.collection.parallel.mutable.ParArray

object ShortestCycles {
    def main(args: Array[String]): Unit = {
        System.setProperty("hadoop.home.dir", "C:\\hadoop\\")
        val conf = new SparkConf().setAppName("Test").setMaster("local")
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        GraphXUtils.registerKryoClasses(conf)
        val sc = new SparkContext(conf)
        sc.setLogLevel("ERROR")

        val g = Graph.fromEdges(
      sc.makeRDD(Array(
        Edge(1L,2L,1), Edge(1L,3L,1), Edge(1L,4L,1), Edge(1L,11L,1), Edge(1L,22L,1),
        Edge(2L,3L,1), Edge(2L,4L,1),
        Edge(3L,4L,1),
        Edge(5L, 1L, 1),Edge(5L, 2L, 1),Edge(5L, 3L, 1),Edge(5L, 4L, 1),
        Edge(6L, 1L, 1),Edge(6L, 2L, 1),Edge(6L, 3L, 1),Edge(6L, 4L, 1), Edge(6L, 5L, 1)
      )), defaultValue = 1)
    val shortestCycles = apply(g)
    println(shortestCycles.mkString("\n"))
    }

    def generateCycle(seq: Seq[VertexId], edgeList: Set[(VertexId, VertexId)]) = {
        val cycleEdges = seq.zipWithIndex.init.map(v => {
            val idx = v._2
            val e = (v._1, seq(idx + 1))
            if (edgeList.contains(e)) {
                Edge(v._1, seq(idx + 1), true)
            }
            else {
                Edge(seq(idx + 1), v._1, true)
            }
        })
        new Cycle(cycleEdges, seq)
    }

    def apply(p: Graph[Int, Int]): mutable.Set[Cycle] = {
        require(p.numEdges > 0, "Graph edges must be > 0")
        p.cache()

        val edges = p.edges.collect
        val edgeList = edges.map(e => (e.srcId, e.dstId)).toSet
        val edgeIterator = edges.toIterator
        val cycles = scala.collection.mutable.Set[Cycle]()

        do {
            val edge = edgeIterator.next()
//            println("Current edge is "+ edge)
            val g = p.subgraph(epred = _ != edge)
            val a = g.collectNeighborIds(EdgeDirection.Either).collectAsMap()
            val shortestPaths = bfs(edge.dstId, edge.srcId, a)
//            println("Path from " + edge.srcId + " to " + edge.dstId + " is : " + shortestPaths.mkString(", "))
            if (shortestPaths.nonEmpty) cycles ++= shortestPaths.map(path => generateCycle(path :+ edge.dstId, edgeList))
        } while (edgeIterator.hasNext)
        cycles

    }

    def bfs(src: VertexId, dst: VertexId, a: scala.collection.Map[VertexId, Array[VertexId]]): Array[mutable.Seq[VertexId]] = {
        if (!a.contains(dst)) return Array.empty
        val visited = mutable.Map(a.keySet.map(_ -> false).toSeq: _*)
        var dstFound = false
        var shortestPaths = Array[mutable.Seq[VertexId]]()
        var paths = ParArray(mutable.Seq(src))
        visited(src) = true
        var newlyVisited = 0
        var previouslyVisited = 0

        do {
            previouslyVisited = newlyVisited
            paths = paths.flatMap(path => {
                /**
                  * map current paths with neighbors of the last visited vertices
                  * in the list of all paths
                  */
                a(path.last).map(n => {
                    visited(n) = true
                    path :+ n
                })
            })

            /**
              * shortest paths are the paths that reach dst at the same time
              */
            shortestPaths = paths.map(path => if (path.last == dst) path else mutable.Seq[VertexId]()).filter(_.nonEmpty).toArray
            if (shortestPaths.nonEmpty) {
                dstFound = true
            }

            /**
              * it's important to count newly visited nodes to avoid running infinitely
              * when there are isolated vertices that are impossible to visit
              * because they belong to a different connected component of the graph
              */
            newlyVisited = visited.map(e => if (e._2) 1 else 0).sum

        } while (newlyVisited - previouslyVisited > 0 && !dstFound && !visited.values.forall(_ == true))

        shortestPaths
    }

}
