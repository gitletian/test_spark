package com.marcpoint

import org.apache.spark.{SparkConf, SparkContext}
import scala.math.random

object SparkPi {
	def main(args: Array[String]): Unit = {
		val conf = new SparkConf().setAppName("Spark Pi").setMaster("local[2]")
		val sc = new SparkContext(conf)

		println(sc.applicationId)
		val slices = if(args.length > 0) args(0).toInt else 2
		val n = math.min(100000L * slices, Int.MaxValue).toInt
		val count = sc.parallelize(1 until n, slices).map( i => {
			val x = random * 2 - 1
			val y = random * 2 - 1
			if (x * x + y * y < 1) 1 else 0
		}
		).reduce(_ + _)

		var count1 = sc.parallelize(1 until n, slices).map {
			i =>
				val x = random * 2 - 1
				val y = random * 2 - 1
				if (x * x + y * y < 1) 1 else 0
		}.reduce(_ + _)

		println("PI is roughly " + 4.0 * count / n)
		println("PI is roughly " + 4.0 * count1 / n)
		sc.stop()
	}
}
