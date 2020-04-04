import org.apache.spark._

object MergeSort {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir","C:\\Installations\\Hadoop")
    val conf = new SparkConf().setAppName("MergeSort").setMaster("local[*]")
    val sc = new SparkContext(conf)
    println(mergeSort(List(38,27,43,3,9,82,10)))
  }

  def mergeSort(numbers: List[Int]): List[Int] = {
    val halfLength = numbers.length / 2
    if(halfLength == 0) {
      numbers
    }
    else{
      val (left, right) = numbers splitAt(halfLength)
      merge(mergeSort(left), mergeSort(right))
    }
  }

  def merge(firstHalf: List[Int], secondHalf: List[Int]) : List[Int] =
    (firstHalf, secondHalf) match {
      case(Nil, secondHalf) => secondHalf
      case(firstHalf, Nil) => firstHalf
      case(x :: fh1, y:: sh1) =>
        if( x < y) x:: merge(fh1, secondHalf)
        else y :: merge(firstHalf, sh1)
    }

}
