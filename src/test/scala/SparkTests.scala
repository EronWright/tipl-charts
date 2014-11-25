package tipl.charts

/**
 * Based on the implementation from the Spark Testing Suite, a set of commands to initialize a
 * Spark context for tests
 * Created by mader on 10/10/14.
 */


import java.io.File

import _root_.io.netty.util.internal.logging.{InternalLoggerFactory, Slf4JLoggerFactory}
import com.google.common.io.Files
import org.scalatest.{FunSuite, BeforeAndAfterAll, BeforeAndAfterEach, Suite}
import org.apache.spark.SparkContext
import tipl.spark.SparkGlobal
import tipl.util.{TypedPath, TIPLGlobal}


/** Manages a local `sc` {@link SparkContext} variable, correctly stopping it after each test. */
trait LocalSparkContext extends BeforeAndAfterEach with BeforeAndAfterAll {
  self: Suite =>

  @transient var sc: SparkContext = _

  override def beforeAll() {
    InternalLoggerFactory.setDefaultFactory(new Slf4JLoggerFactory())
    super.beforeAll()
  }

  override def afterEach() {
    resetSparkContext()
    super.afterEach()
  }

  def resetSparkContext() = {
    LocalSparkContext.stop(sc)
    sc = null
  }

  def getSpark(testName: String) = SparkGlobal.getContext("Testing:" + testName).sc

}


object LocalSparkContext {
  def stop(sc: SparkContext) {
    SparkGlobal.stopContext()
    // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
    System.clearProperty("spark.driver.port")
  }

  /** Runs `f` by passing in `sc` and ensures that `sc` is stopped. */
  def withSpark[T](sc: SparkContext)(f: SparkContext => T) = {
    try {
      f(sc)
    } finally {
      stop(sc)
    }
  }

}

/**
 * Created by mader on 10/27/14.
 */
class SparkTests extends FunSuite with LocalSparkContext {

  var tempDir: File = _

  override def beforeEach() {
    super.beforeEach()
    tempDir = Files.createTempDir()
    tempDir.deleteOnExit()
  }

  override def afterEach() {
    super.afterEach()
    TIPLGlobal.RecursivelyDelete(TypedPath.localFile(tempDir))
  }

  test("Check if we even get a sparkcontext") {
    sc = getSpark("Acquiring context...")
    println("Current context is named: " + sc.appName)
  }

  test("Make a basic flat chart") {
    sc = getSpark("plotTest")
    import tipl.charts.spark._
    val b = sc.parallelize(1 to 1000).map(_*1.0)
    println(b.showHistogram().mkString(","))
  }

  test("Make a quadratic distribution") {
    sc = getSpark("plotTest_x^2")
    import tipl.charts.spark._
    val b = sc.parallelize(1 to 1000).map(math.pow(_,2))
    println(b.showHistogram().mkString(","))
  }

  test("Make a few distributions") {
    sc = getSpark("plotTest_x^2")
    import tipl.charts.spark._
    val a = sc.parallelize(1 to 1000).map(av => ("a",av*1.0))
    val b = sc.parallelize(1 to 1000).map(bv => ("b",math.pow(bv,2)))
    new manyDoubleRDD(a.union(b)).showHistograms()
    //TODO this should work as well
    //println(a.union(b).showHistogram().mkString(","))
  }


}