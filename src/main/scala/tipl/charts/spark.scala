package tipl.charts

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag
import scalax.chart.module.ChartFactories.XYBarChart

/**
 * A collection of visualizations for RDD objects in Spark
 * Created by mader on 10/27/14.
 */
object spark {

  implicit val theme = org.jfree.chart.StandardChartTheme.createDarknessTheme
  /** enhancing the doubleRDD tools
    *
    * @param inRdd
    */
  implicit class plottableDoubleRDD(inRdd: RDD[Double]) {
    def showHistogram(bucketCount: Int = 20,title: String ="") = {
      val hist = inRdd.histogram(bucketCount)
      val plotData = for (x <- hist._1.zip(hist._2)) yield (x._1.toDouble,x._2.toDouble)
      val chart = XYBarChart(plotData.toIndexedSeq,title=title)
      chart.show()
      plotData
    }

  }

  /** enhancing the doubleRDD tools
    *
    * @param inRdd
    */
  implicit class manyDoubleRDD[T](inRdd: RDD[(T,Double)])(implicit tm: ClassTag[T]) {
    def showHistograms(bucketCount: Int = 20,title: String="") = {
      val keys = inRdd.map(_._1).distinct().collect()
      val tPlot = keys.map{
        ckey =>
          val hist = inRdd.filter(_._1==ckey).map(_._2).histogram(bucketCount)
          (ckey.toString,
            (for (x <- hist._1.zip(hist._2)) yield (x._1.toDouble,x._2.toDouble)).toIndexedSeq)
      }
      val chart = XYBarChart.stacked(tPlot.toIndexedSeq,title=title)
      chart.show()
      tPlot
    }

  }
}
