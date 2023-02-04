# Bounded Dual Simulation 
Bounded Dual Simulation (BDSim) is a flexible graph pattern matching algorithm that captures the cyclic structure of a query graph. 

## Abstract
Graph Pattern Matching (GPM) finds subgraphs of a large data graph that are similar to an input query graph. It has many applications, such as pattern recognition, detecting plagiarism, and finding communities in social networks. 
Current real-world applications generate massive amounts of data that cannot be stored on the memory of a single machine, which raises the need for distributed storage and processing. 
Recent relaxed GPM models, although of polynomial time complexity, are nevertheless not distributed by nature. Moreover, the existing relaxed GPM algorithms are limited in terms of scalability. We propose Bounded Dual Simulation (BDSim) as a new relaxed model for a scalable evaluation of GPM in massive graphs. 
BDSim captures more semantic similarities compared to graph simulation, dual simulation, and even strong simulation. It preserves the vertices’ proximity by eliminating cycles of unbounded length from the resulting match graph. Furthermore, we propose distributed vertex-centric algorithms to evaluate BDSim. 
We prove their effectiveness and efficiency through detailed theoretical validation and extensive experiments conducted on real-world and synthetic datasets. To the best of our knowledge, BDSim is the first relaxed GPM model that captures the cyclic structure of the query graph while being feasible in cubic time.

## Implementation
This repository provides a distributed implementation (in Scala) of our approach over the distributed graph processing framework GraphX. 
The entrypoint is the class ```gpm.models.BDualSim``` where an example is given on how to call the GPM algorithm on a graph and a pattern of the type Graph[Int, Int]. You can get the performance measures: running time, number of super-steps, number of messages, and the number of active vertices by super-step by running the following code. 

```scala
        /** launching Spark*/
        val conf = new SparkConf().setAppName("Test BDSim").setMaster("local[*]")
        val sc = new SparkContext(conf)
        sc.setLogLevel("ERROR")

        /** loading a sample graph and pattern*/ 
        val g = Util.graph(sc)
        val p = Util.pattern(sc)
        
        /** setting the parameter k of BDSim */
        val k: Byte = 1

        /** computing the set of shortest cycles of the pattern graph */
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
        
        /** reporting the different performance measures */
        println("running time " + runningTime + " ms")
        println("num super-steps = " + steps)
        println("num exchanged messages = " + msgsCount)
        println("num active vertices by super-step = " + Util.toSortedString(activeVertices))

        sc.stop()

```
More details on the algorithm and explanations are provided in the original reasearch paper that you can find here: [Distributed graph pattern matching via bounded dual simulation](https://doi.org/10.1016/j.ins.2022.08.038)

Please cite this work as follows when using the code provided in this repository. 

`Bouhenni, S., Yahiaoui, S., Nouali-Taboudjemat, N., & Kheddouci, H. (2022). Distributed graph pattern matching via bounded dual simulation. Information Sciences, 610, 549-570.`
