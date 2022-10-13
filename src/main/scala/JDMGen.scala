import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

case class Edge (source:String, destination:String )
case class Quad (source:String, sDegree:Long, dDegree:Long, destination:String)

object JDMGen {

  def main(args : Array[String]): Unit ={
    Logger.getRootLogger.setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]" , "JDMGen");
    //val edgelistFile = sc.textFile("/Users/mehmet/Desktop/sampleDatasets/facebook_combined.txt")
    ////////
    val edgelistFile = sc.textFile("/Users/mehmet/workspace/DegreeCorrelations/src/main/resources/followers.txt")
    val edgeList = parseEdgeList(edgelistFile)
    edgeList.collect().foreach(println)
    println("---------")
    val degree= computeDegree(edgeList)
    val quads = edgeListToQuads(edgeList, degree)
    quads.collect().foreach(println)
    println("---------")

    val anonimizedQuads = anonymizeQuads(quads)
    anonimizedQuads.collect().foreach(println)
    println("---------")

    val splittedQuads =  splitQuads(anonimizedQuads)
    splittedQuads.collect().foreach(println)
    println("---------")

    val nodeMappings = computeNodeMappings(splittedQuads)
    nodeMappings.collect().foreach(println)
    println("---------")

//    val simplifiedNodeMappings = simplifyNodeMappings(nodeMappings)
//    simplifiedNodeMappings.collect().foreach(println)
//    println("---------")


    val renamedEdges= anonimizedQuads
      .map(e => (e.source,Edge(e.source,e.destination)))
      .join(nodeMappings)
      .map(e => (e._2._1.destination , Edge(e._2._2, e._2._1.destination)))
      .join(nodeMappings)
      .map(e => Edge(e._2._1.source, e._1))


    renamedEdges.collect().foreach(println)
    println("---------")

    Thread.sleep(500000)
  }

  def simplifyNodeMappings(nodeMappings: RDD[(String, String)]) : RDD[(String, Long)] = {
    val simplifiediIndices = nodeMappings
      .map(e => e._2)
      .distinct()
      .zipWithIndex()

    val simplifiedNodeMappings = nodeMappings
      .map(e => (e._2,e._1))
      .join(simplifiediIndices)
      .map{e => (e._2._1, e._2._2)}

    simplifiedNodeMappings
  }

  def computeNodeMappings(splittedQuads:RDD[(Long,String)]) : RDD[(String,String)] = {
    val x =
      splittedQuads.groupBy(e=>e._1).map{
        e => {
          val size = e._2.size
          val degree = e._1
          val arr: Array[String] = new Array(size)
          e._2.map(p=>p._2).copyToArray(arr)

          //randomize the order of nodeIds in the array
          for(i <- 0 until arr.length) {
            val temp = arr(i)
            val rand = Math.random() % arr.length
            arr(i) = arr(rand.toInt)
            arr(rand.toInt) = temp
          }

          if (size%degree != 0 ) {
            println("Should be an exact division; degree=" + degree +  "  nodeCount=" + size)
            println("Might be caused by self edges in the input")
          }

          //group the entries into blocks of size:degree
          //for each block pick the first id as target id
          //we will reconcile all nodes within the group to targetId
          //emit a mapping from each id to he target id
          val mappings = arr.groupBy( entry => (arr.indexOf(entry) / degree).floor )
            .flatMap{block => {
              val ids = block._2
              val targetIdInBlock = ids(0)
              ids.map(id => (id,targetIdInBlock))
              }
            }


          (e._1,arr.mkString(" "), mappings)
        }
      }
//        x.collect().foreach(println)
      x.flatMap(_._3)
  }

  def splitQuads(anonymizedQuads:RDD[Quad]) : RDD[(Long,String)] = {
    val e1 = anonymizedQuads.map(e => (e.sDegree,e.source))
    val e2 = anonymizedQuads.map(e => (e.dDegree,e.destination))
    e1.union(e2).map{e:(Long,String) => e}
  }

  def anonymizeQuads(quads:RDD[Quad]) : RDD[Quad] = {
    quads.zipWithUniqueId()
      .map{e => Quad((2 * e._2).toString,e._1.sDegree ,e._1.dDegree , (2 * e._2+1).toString)}
  }

  def edgeListToQuads(edgeList:RDD[Edge], degree:RDD[(String,Int)]): RDD[Quad] = {
    edgeList
      .map{edge:Edge => (edge.source , Quad(edge.source,0,0,edge.destination))}
      .join(degree)
      .map(e => (e._2._1.destination , Quad(e._2._1.source, e._2._2, 0, e._2._1.destination)))
      .join(degree)
      .map(e => Quad(e._2._1.source, e._2._1.sDegree, e._2._2, e._2._1.destination))
  }

  def computeDegree(edgeList:RDD[Edge]):RDD[(String,Int)] = {
    val e1 =edgeList.map{edge:Edge => (edge.source,1)}
    val e2 =edgeList.map{edge:Edge => (edge.destination,1)}
    e1.union(e2).reduceByKey((x,y) => x + y)
  }

  def parseEdgeList(edgelistFile: RDD[String]) : RDD[Edge] = {
    edgelistFile.
      map(line => line.split(' ')).
      filter( edge => edge.length == 2 ).
      map{edge:Array[String] => Edge(edge(0),edge(1))}
  }
}
