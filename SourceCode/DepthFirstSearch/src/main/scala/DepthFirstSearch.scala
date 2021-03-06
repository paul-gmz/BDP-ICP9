
import org.apache.spark.{SparkConf, SparkContext}

object DepthFirstSearch {
    def main(args: Array[String]) {
      System.setProperty("hadoop.home.dir","C:\\Installations\\Hadoop")
      val conf = new SparkConf().setAppName("DepthFirstSearch").setMaster("local[*]")
      val sc = new SparkContext(conf)
      type Vertex = Int
      type Graph = Map[Vertex, List[Vertex]]
      val g: Graph = Map(1 -> List(2,3,5,6,7), 2 -> List(1,3,4,6,7), 3 -> List(1,2), 4 -> List(2,5,7),5 -> List(1,6,7),6 -> List(1,2,5,7),7 -> List(1,2,4,5,6))

      def DFS(start: Vertex, g: Graph): List[Vertex] = {
        def DFS0(v: Vertex, visited: List[Vertex]): List[Vertex] = {
          if(visited.contains(v)) visited
          else{
            val neighbors:List[Vertex] = g(v).filterNot(visited.contains)
            neighbors.foldLeft(v :: visited)((b,a) => DFS0(a,b))
          }
        }
        DFS0(start,List()).reverse
      }

      val dfsResult = DFS(1,g)
      println("DFS Output ", dfsResult.mkString(","))
    }
}
