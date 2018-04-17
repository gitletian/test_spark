package com.marcpoint

import java.util.Properties

import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}


object TouchPoint {

	var daterange: String = "2018-01-15"
	var mysql_host =  "172.16.1.100"

	lazy val url: String = s"jdbc:postgresql://${mysql_host}:5432/media_choice"
	val prop: Properties = {
		val prop = new Properties()
		prop.setProperty("user", "media_choice")
		prop.setProperty("password", "Marcpoint2016")
		prop.put("driver","org.postgresql.Driver")
		prop
	}

	// 各种udf
	def iff = udf((a: Boolean, b: String, c: String) => if(a) b else c)
	def ifa = udf((a: Boolean, b: Seq[String], c: Seq[String]) => if(a) b else c)
	def arl = udf((a: Seq[String]) => if(a == null) null else a.last)

	/*
	* 计算指标
	*
	* */
	def get_count_data(tp_scene_point_attr: DataFrame, tp_people_angle: DataFrame, hiveContext: HiveContext)(row: Row): Unit ={
		import hiveContext.implicits._

		/*************************************** 初始化提取数据 *************************************************/

		val people_type: String = row.getAs[String]("type_code")
		val regex_rule: String = row.getAs[String]("value")

		println(s"people_type is ${people_type}")
		println(s"regex_rule is ${regex_rule}")

		val tpa_filter = tp_people_angle.filter($"people_type" === people_type && array_contains($"regex_rule", regex_rule))
		if (tpa_filter.takeAsList(1).isEmpty) return

		val tp_people_angle_filter = tp_scene_point_attr.join(tpa_filter, Seq("platform_id", "user_id"), "leftsemi")
			.select($"id", $"user_id", $"attr", $"platform_id", $"categoryids", $"brands", size(split($"attr", "\\.")).as("attr_len"))
		tp_people_angle_filter.cache()

		if (tp_people_angle_filter.takeAsList(1).isEmpty) return

		val scene_table = tp_people_angle_filter.filter($"attr".rlike("场景\\.") && $"attr_len" === 2)
			.select($"id", $"user_id", split($"attr", "\\.")(1).as("scene"), $"platform_id", $"categoryids", $"brands", $"attr_len").alias("a")
		scene_table.cache()

		val tp_table = tp_people_angle_filter.filter($"attr".rlike("TP\\.")).alias("b")

		val tp_scene_tp_filter = scene_table.join(tp_table, $"a.platform_id" === $"b.platform_id" && $"a.id" === $"b.id", "outer")
			.select(
				iff($"b.id".isNull, $"a.id", $"b.id").as("id")
				, iff($"b.id".isNull, $"a.user_id", $"b.user_id").as("user_id")
				, $"a.scene"
				, iff($"b.id".isNull, $"a.platform_id", $"b.platform_id").as("platform_id")
				, ifa($"b.id".isNull, $"a.categoryids", $"b.categoryids").as("categoryids")
				, ifa($"b.id".isNull, $"a.brands", $"b.brands").as("brands")
				, $"b.attr".as("tp")
				, $"b.attr_len"
			).repartition(573, expr("id"))

		tp_scene_tp_filter.cache()

		/*************************************** 计算场景触点 *************************************************/

		// 1、生成场景数据
		val inside_track_table = tp_people_angle_filter.filter($"attr".rlike("TP内圈\\.")).alias("c")
		val scene_inside_track_table = scene_table.join(inside_track_table, $"a.platform_id" === $"c.platform_id" && $"a.id" === $"c.id", "left")
			.filter($"c.id".isNotNull).select($"a.id", $"a.user_id", $"a.scene", $"a.platform_id", $"c.attr".as("inside_track")).alias("d")

		val scene_inside = scene_inside_track_table.groupBy($"scene").agg(countDistinct("user_id").as("inside_track_user"))
		val scene_inside_A = scene_inside_track_table.filter($"inside_track".like("%场景A%")).groupBy($"scene").agg(countDistinct("user_id").as("inside_track_a_user"))
		val scene_inside_B = scene_inside_track_table.filter($"inside_track".like("%场景B%")).groupBy($"scene").agg(countDistinct("user_id").as("inside_track_b_user"))

		val scene_tp_scene_user_count = scene_table.filter($"scene".isNotNull).groupBy($"scene").agg(countDistinct("user_id").as("user_count"))
		val scene_tp_tp_user = tp_scene_tp_filter.filter($"tp".isNotNull && $"scene".isNotNull).groupBy($"scene").agg(countDistinct("user_id").as("tp_user"))
		val all_scene_user_count = scene_table.agg(countDistinct("user_id").as("all_scene_user"))

		val tpa_scene_export = scene_tp_scene_user_count.join(scene_tp_tp_user, Seq("scene"), "left_outer")
			.join(scene_inside, Seq("scene"), "left_outer")
			.join(scene_inside_A, Seq("scene"), "left_outer")
			.join(scene_inside_B, Seq("scene"), "left_outer")
			.join(all_scene_user_count)

		df2pg(tpa_scene_export, "tpa_scene_data", regex_rule, hiveContext)

		// 2、生成场景品牌数据
		val scene_brand = scene_table.filter($"brands".isNotNull).explode("brands", "t2_brands"){brands: Seq[String] => brands}
			.select($"user_id", $"scene", split($"t2_brands", "_")(0).cast(IntegerType).as("category"), split($"t2_brands", "_")(1).cast(IntegerType).as("brand"))

		val scene_brand_user_count = scene_brand.groupBy("scene", "category", "brand").agg(countDistinct("user_id").as("user_count"))
		val scene_brand_all_user = scene_brand.groupBy("scene", "category").agg(countDistinct("user_id").as("brand_all_user"))

		val tpa_scene_brand_export = scene_brand_all_user.join(scene_brand_user_count, Seq("scene", "category"), "left_outer")
		df2pg(tpa_scene_brand_export, "tpa_scene_brand_data", regex_rule, hiveContext)

		// 3、生成子场景数据
		val sub_scene = tp_people_angle_filter.filter($"attr".rlike("场景\\.") && $"attr_len" === 3)
			.select($"id", $"user_id", $"categoryids", split($"attr", "\\.")(1).as("parent_scene"), split($"attr", "\\.")(2).as("scene"))
		val tpa_subscene_export = sub_scene.groupBy("parent_scene", "scene").agg(countDistinct("user_id").as("user_count"), countDistinct("id").as("id_count"))
		df2pg(tpa_subscene_export, "tpa_subscene_data", regex_rule, hiveContext)

		// 4、生成子场景品类数据
		val tpa_subscene_category_export = sub_scene.filter($"categoryids".isNotNull).explode("categoryids", "category"){c: Seq[String] => c}
			.groupBy($"parent_scene", $"scene", $"category").agg(countDistinct("id").as("id_count"))
			.select($"parent_scene", $"scene", $"category".cast(IntegerType), $"id_count")
		df2pg(tpa_subscene_category_export, "tpa_subscene_category_data", regex_rule, hiveContext)
		/*
		* 基础数据
		* */
		val scene_tp = tp_scene_tp_filter.filter($"tp".isNotNull && $"attr_len".isin(2, 3))
			.select($"scene", $"user_id", $"brands", $"categoryids", $"attr_len"
				, when($"attr_len" === 2, "self").otherwise(split($"tp", "\\.")(1)).as("parent_tp"), arl(split($"tp", "\\.")).as("tp"))

		val scene_tp_brand = scene_tp.filter($"attr_len" === 2 && $"brands".isNotNull).explode("brands", "brandsnew"){a: Seq[String] => a}
			.select($"scene", $"tp", $"user_id", split($"brandsnew", "_")(0).cast(IntegerType).as("category"), split($"brandsnew", "_")(1).cast(IntegerType).as("brand"))

		val tp_category = scene_tp.filter($"attr_len" === 2 && $"categoryids".isNotNull)
			.explode("categoryids", "category"){c: Seq[String] => c}.select($"scene", $"user_id", $"tp", $"category".cast(IntegerType))

		// 5、生成场景触点数据
		val tpa_scene_tp_export = scene_tp.filter($"scene".isNotNull).groupBy("scene", "parent_tp", "tp").agg(countDistinct("user_id").as("user_count"))
		df2pg(tpa_scene_tp_export, "tpa_scene_tp_data", regex_rule, hiveContext)

		// 6、生成场景触点品牌数据
		val scene_tp_brand_user_count = scene_tp_brand.filter($"scene".isNotNull).groupBy("scene", "tp", "category", "brand").agg(countDistinct("user_id").as("user_count"))
		val scene_tp_brand_all_user = scene_tp_brand.filter($"scene".isNotNull).groupBy("scene", "tp", "category").agg(countDistinct("user_id").as("brand_all_user"))

		val tpa_scene_tp_brand_export = scene_tp_brand_all_user.join(scene_tp_brand_user_count, Seq("scene", "tp", "category"), "left_outer")
		df2pg(tpa_scene_tp_brand_export, "tpa_scene_tp_brand_data", regex_rule, hiveContext)
		// 7、生成触点数据
		val tp = tp_scene_tp_filter.filter($"tp".rlike("TP\\.") && $"attr_len" === 2).select(split($"tp", "\\.")(1).as("tp"), $"user_id")
		val tp_user_count = tp.groupBy("tp").agg(countDistinct("user_id").as("user_count"))
		val tp_all_user = tp.agg(countDistinct("user_id").as("tp_all_user"))
		val tp_scene_user = scene_tp.filter($"attr_len" === 2 && $"scene".isNotNull).groupBy("tp").agg(countDistinct("user_id").as("scene_user"))

		val tpa_tp_export = tp_user_count.join(tp_scene_user, Seq("tp"), "left_outer").join(tp_all_user)
		df2pg(tpa_tp_export, "tpa_tp_data", regex_rule, hiveContext)
		// 8、生成触点品牌
		val tpa_tp_brand_export = scene_tp_brand.groupBy("tp", "category", "brand").agg(countDistinct("user_id").as("user_count"))
		df2pg(tpa_tp_brand_export, "tpa_tp_brand_data", regex_rule, hiveContext)
		// 9、生成触点品类
		val tp_brand_all_user = scene_tp_brand.groupBy("category", "tp").agg(countDistinct("user_id").as("brand_all_user"))

		val tp_category_user_count = tp_category.groupBy("category", "tp").agg(countDistinct("user_id").as("user_count"))
		val tp_category_tp_all_user = tp_category.groupBy("category").agg(countDistinct("user_id").as("tp_all_user"))

		val tpa_tp_category_export = tp_category_tp_all_user.join(tp_category_user_count, Seq("category"), "left_outer")
			.join(tp_brand_all_user, Seq("category", "tp"), "left_outer")
		df2pg(tpa_tp_category_export, "tpa_tp_category_data", regex_rule, hiveContext)

		/*************************************** 数据导出 *************************************************/
		tp_people_angle_filter.unpersist
		tp_scene_tp_filter.unpersist
		scene_table.unpersist
	}

	/*＊
	* 将数据导入pg
	* */
	def df2pg(df: DataFrame, tablename: String, regex_rule: String, hiveContext: HiveContext): Unit = {
		val columns = hiveContext.read.format("jdbc").jdbc(url, tablename, prop).columns
		df.na.fill(0).withColumn("people_angle", lit(regex_rule)).withColumn("daterange", to_date(lit(daterange)))
			.selectExpr(columns: _*).coalesce(53).write.mode(SaveMode.Append).jdbc(url, tablename, prop)
	}

	/*＊
	* 主方法
	* memoryOverhead = sparkConf.getInt("spark.yarn.executor.memoryOverhead", math.max((MEMORY_OVERHEAD_FACTOR * executorMemory).toInt, MEMORY_OVERHEAD_MIN))
	*
	* */
	def main(args: Array[String]): Unit = {

		val strat: Long = System.currentTimeMillis

		val conf = new SparkConf()
			.setAppName("Touch Point")
			.set("spark.default.parallelism", "635")


		if(args.length > 0) daterange = args(0)
		if(args.length > 1) mysql_host = args(1)

		val sc = new SparkContext(conf)
		val hiveContext = new HiveContext(sc)

		val tp_scene_point_attr = hiveContext.table("media_choice.tp_scene_point_attr").repartition(573, expr("id"))
		val tp_people_angle = hiveContext.table("transforms.tp_people_angle").repartition(573, expr("user_id"))
		tp_scene_point_attr.cache()
		tp_people_angle.cache()

		val data_func = get_count_data(tp_scene_point_attr, tp_people_angle, hiveContext) _
		val tpa_people_angle = hiveContext.read.format("jdbc").jdbc(url, "tpa_people_angle", prop).filter(expr("able"))
		tpa_people_angle.collect().foreach(row => data_func(row))

		tp_scene_point_attr.unpersist
		tp_people_angle.unpersist
		sc.stop()

		println(s"总共花费时间为： ${System.currentTimeMillis - strat}")
	}
}
