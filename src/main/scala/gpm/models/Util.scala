package gpm.models

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph}

import scala.collection.mutable

object Util {
    def graph(sc: SparkContext): Graph[Int, Int] = {
        val vertices = sc.makeRDD(List((1L, 2), (2L, 2), (3L, 1), (4L, 3), (5L, 4), (6L, 5), (7L, 2), (8L, 1), (9L, 3), (10L, 1), (11L, 6), (12L, 4), (13L, 5), (14L, 4), (15L, 5), (16L, 1), (17L, 3), (18L, 4), (19L, 5), (20L, 4), (21L, 5), (22L, 4), (23L, 5), (24L, 4), (25L, 5), (26L, 3), (27L, 6), (28L, 2), (29L, 1), (30L, 3), (31L, 1), (32L, 2), (33L, 5), (34L, 5)))
        val edges = sc.makeRDD(List(Edge(1L, 3L, 1), Edge(2L, 3L, 1), Edge(3L, 4L, 1), Edge(3L, 10L, 1), Edge(5L, 4L, 1), Edge(5L, 6L, 1), Edge(6L, 5L, 1), Edge(7L, 8L, 1), Edge(8L, 9L, 1), Edge(10L, 3L, 1), Edge(10L, 9L, 1), Edge(10L, 11L, 1), Edge(12L, 9L, 1), Edge(12L, 13L, 1), Edge(13L, 14L, 1), Edge(14L, 15L, 1), Edge(14L, 26L, 1), Edge(15L, 12L, 1), Edge(16L, 9L, 1), Edge(16L, 17L, 1), Edge(18L, 17L, 1), Edge(18L, 19L, 1), Edge(19L, 20L, 1), Edge(20L, 21L, 1), Edge(20L, 30L, 1), Edge(21L, 22L, 1), Edge(22L, 23L, 1), Edge(22L, 26L, 1), Edge(22L, 34L, 1), Edge(23L, 24L, 1), Edge(24L, 9L, 1), Edge(24L, 25L, 1), Edge(25L, 18L, 1), Edge(26L, 27L, 1), Edge(26L, 28L, 1), Edge(28L, 29L, 1), Edge(29L, 26L, 1), Edge(31L, 30L, 1), Edge(32L, 16L, 1), Edge(32L, 31L, 1), Edge(33L, 18L, 1)))
        Graph(vertices, edges)
    }

    def pattern(sc: SparkContext): Graph[Int, Int] = {
        val vertices = sc.makeRDD(List((1L, 1), (2L, 2), (3L, 3), (4L, 4), (5L, 5)))
        val edges = sc.makeRDD(List(Edge(2L, 1L, 1), Edge(1L, 3L, 1), Edge(4L, 3L, 1), Edge(4L, 5L, 1), Edge(5L, 4L, 1)))
        Graph(vertices, edges)
    }

    def toString(hmap: mutable.Map[Byte, Long]): String = hmap.map { case (k, v) => k + ":" + v + "-" }.mkString.init

    def toSortedString(hmap: mutable.Map[Byte, Long]): String = hmap.toSeq.sorted.map { case (iteration, activeVertices) => iteration + " -> " + activeVertices }.mkString(", ")

}
