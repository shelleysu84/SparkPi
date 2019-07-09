import org.apache.spark.{SparkConf, SparkContext}
import scala.math.{pow, random}

/** Computes an approximation to pi */
object SparkPi {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SP test for PI calc")
      /*for test on local machine*/
//       .setMaster("local[*]")
    val sc = new SparkContext(conf)
    val slices = 100
    val n = math.min(100000L * slices, Int.MaxValue).toInt
    val count = sc.parallelize(1 until n, slices).map { i =>
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (pow(x, 2) + pow(y, 2) <= 1) 1 else 0
    }.reduce(_ + _)
    println("Pi is roughly " + 4.0 * count / (n - 1))
    sc.stop()
  }
}
