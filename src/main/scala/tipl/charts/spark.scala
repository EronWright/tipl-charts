package tipl.charts

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import scalax.chart.module.ChartFactories.XYLineChart

/**
 * A collection of visualizations for RDD objects in Spark
 * Created by mader on 10/27/14.
 */
object spark {


  /** enhancing the doubleRDD tools
    *
    * @param inRdd
    */
  implicit class plottableDoubleRDD(inRdd: RDD[Double]) {
    def showHistogram(bucketCount: Int = 20) = {
      val hist = inRdd.histogram(bucketCount)
      val plotData = for (x <- hist._1.zip(hist._2)) yield (x._1.toDouble,x._2.toDouble)
      val chart = XYLineChart(plotData.toIndexedSeq)
      chart.show()
    }

  }
}
