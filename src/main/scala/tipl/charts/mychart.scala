package tipl.charts

import scalax.chart.api._

object MyChartApp extends App with scalax.chart.module.Charting {
  val data = for (i <- 1 to 5) yield (i,i)
  val chart = XYLineChart(data)
  chart.saveAsPNG("/tmp/chart.png")
}