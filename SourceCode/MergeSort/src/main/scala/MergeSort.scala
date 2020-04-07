import org.apache.spark._

object MergeSort {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir","C:\\Installations\\Hadoop")
    val conf = new SparkConf().setAppName("MergeSort").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val input = List(38,27,43,3,9,82,10)
    val output = sc.parallelize(Seq(input)).map(mergeSort).collect().toList
    println(output);
  }

  def mergeSort(lst: List[Int]): List[Int] = lst match  {
    case Nil => lst
    case h::Nil => lst
    case _ =>
      val (l1, l2) = lst.splitAt(lst.length/2)
      merge(mergeSort(l1), mergeSort(l2))
  }

  def merge(l1: List[Int], l2: List[Int]) : List[Int] =
    (l1, l2) match {
      case(Nil, _) => l2
      case(_, Nil) => l1
      case(h1 :: t1, h2:: t2) =>
        if( h1 < h2) h1::merge(t1, l2)
        else h2::merge(l1,t2 )
    }

}
