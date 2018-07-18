package com.marcpoint

import java.util.Properties

import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.{StorageLevel}


object TouchPoint {

	var daterange: String = "2018-03-15"
	var mysql_host =  "172.16.1.100"

	var sc: SparkContext = _
	lazy val hiveContext: HiveContext = new HiveContext(sc)

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
	def get_count_data(tp_scene_point_attr: DataFrame, tp_people_angle: DataFrame, data_model: Int)(row: Row): Unit ={
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
			.repartition(857)
		tp_people_angle_filter.persist(StorageLevel.MEMORY_AND_DISK_SER)

		if (tp_people_angle_filter.takeAsList(1).isEmpty) return

		val scene_table = tp_people_angle_filter.filter($"attr".rlike("场景\\.") && $"attr_len" === 2)
			.select($"id", $"user_id", split($"attr", "\\.")(1).as("scene"), $"platform_id", $"categoryids", $"brands", $"attr_len")
			.alias("a")
		scene_table.persist(StorageLevel.MEMORY_AND_DISK_SER)

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
			)

		tp_scene_tp_filter.persist(StorageLevel.MEMORY_AND_DISK_SER)

		/*************************************** 计算场景触点 *************************************************/

		// 1、生成场景数据
		val inside_track_table = tp_people_angle_filter.filter($"attr".rlike("TP内圈\\.")).alias("c")
		val scene_inside_track_table = scene_table.join(inside_track_table, $"a.platform_id" === $"c.platform_id" && $"a.id" === $"c.id", "left")
			.filter($"c.id".isNotNull).select($"a.id", $"a.user_id", $"a.scene", $"a.platform_id", $"c.attr".as("inside_track"), $"a.categoryids").alias("d")

		val scene_inside = scene_inside_track_table.groupBy($"scene").agg(
				countDistinct("user_id").as("inside_track_user")
			, countDistinct(when($"inside_track".like("%场景A%"), $"user_id").otherwise(null)).as("inside_track_A_user")
			, countDistinct(when($"inside_track".like("%场景B%"), $"user_id").otherwise(null)).as("inside_track_B_user")
		)

		val scene_tp_scene_user_count = scene_table.filter($"scene".isNotNull).groupBy($"scene").agg(countDistinct("user_id").as("user_count"))
		val scene_tp_tp_user = tp_scene_tp_filter.filter($"tp".isNotNull && $"scene".isNotNull).groupBy($"scene").agg(countDistinct("user_id").as("tp_user"))
		val all_scene_user = scene_table.agg(countDistinct("user_id").as("all_scene_user")).first.getAs[Long](0)

		val tpa_scene_export = scene_tp_scene_user_count.join(scene_tp_tp_user, Seq("scene"), "left_outer")
			.join(scene_inside, Seq("scene"), "left_outer")
			.withColumn("all_scene_user", lit(all_scene_user))

		df2pg(tpa_scene_export, "tpa_scene_data", data_model, regex_rule)

		// 2、生成场景品类数据
		val category_scene_inside = scene_inside_track_table.filter($"categoryids".isNotNull).explode("categoryids", "category") {c: Seq[String] => c }
			.groupBy($"category", $"scene").agg(
				countDistinct("user_id").as("inside_track_user")
			, countDistinct(when($"inside_track".like("%场景A%"), $"user_id").otherwise(null)).as("inside_track_A_user")
			, countDistinct(when($"inside_track".like("%场景B%"), $"user_id").otherwise(null)).as("inside_track_B_user")
		)

		val category_scene_user = scene_table.filter($"categoryids".isNotNull).explode("categoryids", "category") {c: Seq[String] => c }
		val category_all_scene_user = category_scene_user.groupBy($"category").agg(countDistinct("user_id").as("all_scene_user"))
		val category_scene_user_count = category_scene_user.filter($"scene".isNotNull).groupBy($"category", $"scene").agg(countDistinct("user_id").as("user_count"))

		val category_tp_user_count = tp_scene_tp_filter.filter($"categoryids".isNotNull && $"tp".isNotNull && $"scene".isNotNull)
			.explode("categoryids", "category") {c: Seq[String] => c }
			.groupBy($"category", $"scene").agg(countDistinct("user_id").as("tp_user"))

		val tpa_scene_category_export = category_scene_user_count.join(category_tp_user_count, Seq("category","scene"), "left_outer")
			.join(category_scene_inside, Seq("category", "scene"), "left_outer")
			.join(category_all_scene_user, Seq("category"))

		tpa_scene_category_export.show()
		df2pg(tpa_scene_category_export, "tpa_scene_category_data", data_model, regex_rule)

		// 3、生成场景品牌数据
		val scene_brand = scene_table.filter($"brands".isNotNull).explode("brands", "t2_brands"){brands: Seq[String] => brands}
			.select($"user_id", $"scene", split($"t2_brands", "_")(0).cast(IntegerType).as("category"), split($"t2_brands", "_")(1).cast(IntegerType).as("brand"))

		val scene_brand_user_count = scene_brand.groupBy("scene", "category", "brand").agg(countDistinct("user_id").as("user_count"))
		val scene_brand_all_user = scene_brand.groupBy("scene", "category").agg(countDistinct("user_id").as("brand_all_user"))

		val tpa_scene_brand_export = scene_brand_all_user.join(scene_brand_user_count, Seq("scene", "category"), "left_outer")
		df2pg(tpa_scene_brand_export, "tpa_scene_brand_data", data_model, regex_rule)

		// 4、生成子场景数据
		val sub_scene = tp_people_angle_filter.filter($"attr".rlike("场景\\.") && $"attr_len" === 3)
			.select($"id", $"user_id", $"categoryids", split($"attr", "\\.")(1).as("parent_scene"), split($"attr", "\\.")(2).as("scene"))
		val tpa_subscene_export = sub_scene.groupBy("parent_scene", "scene").agg(countDistinct("user_id").as("user_count"), countDistinct("id").as("id_count"))
		df2pg(tpa_subscene_export, "tpa_subscene_data", data_model, regex_rule)

		// 5、生成子场景品类数据
		val tpa_subscene_category_export = sub_scene.filter($"categoryids".isNotNull).explode("categoryids", "category"){c: Seq[String] => c}
			.groupBy($"parent_scene", $"scene", $"category").agg(countDistinct("id").as("id_count"))
			.select($"parent_scene", $"scene", $"category".cast(IntegerType), $"id_count")
		df2pg(tpa_subscene_category_export, "tpa_subscene_category_data", data_model, regex_rule)
		/*
		* 基础数据
		* */
		val scene_tp = tp_scene_tp_filter.filter($"tp".isNotNull && $"attr_len".isin(2, 3))
			.select($"scene", $"user_id", $"brands", $"categoryids", $"attr_len"
				, when($"attr_len" === 2, "self").otherwise(split($"tp", "\\.")(1)).as("parent_tp"), arl(split($"tp", "\\.")).as("tp"))

		scene_tp.persist(StorageLevel.MEMORY_AND_DISK_SER)

		val scene_tp_brand = scene_tp.filter($"attr_len" === 2 && $"brands".isNotNull).explode("brands", "brandsnew"){a: Seq[String] => a}
			.select($"scene", $"tp", $"user_id", split($"brandsnew", "_")(0).cast(IntegerType).as("category"), split($"brandsnew", "_")(1).cast(IntegerType).as("brand"))

		val tp_category = scene_tp.filter($"attr_len" === 2 && $"categoryids".isNotNull)
			.explode("categoryids", "category"){c: Seq[String] => c}.select($"scene", $"user_id", $"tp", $"category".cast(IntegerType))

		// 6、生成场景触点数据
		val tpa_scene_tp_export = scene_tp.filter($"scene".isNotNull).groupBy("scene", "parent_tp", "tp").agg(countDistinct("user_id").as("user_count"))
		df2pg(tpa_scene_tp_export, "tpa_scene_tp_data", data_model, regex_rule)


		// 7、生成场景触点品类数据 （add 20180601）
		val scene_tp_category = scene_tp.filter($"categoryids".isNotNull && $"scene".isNotNull).explode("categoryids", "category"){c: Seq[String] => c}
		scene_tp_category.cache()
		val category_count = scene_tp_category.groupBy("scene", "parent_tp", "tp", "category").agg(countDistinct("user_id").as("user_count"))
		val tp_count = scene_tp_category.groupBy("scene", "category").agg(countDistinct("user_id").as("tp_user_count"))
		val scene_count = scene_tp_category.groupBy("parent_tp", "tp", "category").agg(countDistinct("user_id").as("scene_user_count"))

		val tpa_scene_tp_category_export = category_count.join(tp_count, Seq("category", "scene"), "left_outer")
			.join(scene_count, Seq("category", "parent_tp", "tp"), "left_outer")
			.select($"scene", $"parent_tp", $"tp", $"category".cast(IntegerType).as("category"), $"user_count", $"tp_user_count", $"scene_user_count")

		df2pg(tpa_scene_tp_category_export, "tpa_scene_tp_category_data", data_model, regex_rule)
		scene_tp_category.unpersist()


		// 8、生成场景触点品牌数据
		val scene_tp_brand_user_count = scene_tp_brand.filter($"scene".isNotNull).groupBy("scene", "tp", "category", "brand").agg(countDistinct("user_id").as("user_count"))
		val scene_tp_brand_all_user = scene_tp_brand.filter($"scene".isNotNull).groupBy("scene", "tp", "category").agg(countDistinct("user_id").as("brand_all_user"))

		val tpa_scene_tp_brand_export = scene_tp_brand_all_user.join(scene_tp_brand_user_count, Seq("scene", "tp", "category"), "left_outer")
		df2pg(tpa_scene_tp_brand_export, "tpa_scene_tp_brand_data", data_model, regex_rule)

		// 9、生成触点数据
		val tp = tp_scene_tp_filter.filter($"tp".rlike("TP\\.") && $"attr_len" === 2).select(split($"tp", "\\.")(1).as("tp"), $"user_id")
		val tp_user_count = tp.groupBy("tp").agg(countDistinct("user_id").as("user_count"))
		val tp_all_user = tp.agg(countDistinct("user_id").as("tp_all_user"))
		val tp_scene_user = scene_tp.filter($"attr_len" === 2 && $"scene".isNotNull).groupBy("tp").agg(countDistinct("user_id").as("scene_user"))

		val tpa_tp_export = tp_user_count.join(tp_scene_user, Seq("tp"), "left_outer").join(tp_all_user)
		df2pg(tpa_tp_export, "tpa_tp_data", data_model, regex_rule)

		// 10、生成触点品牌
		val tpa_tp_brand_export = scene_tp_brand.groupBy("tp", "category", "brand").agg(countDistinct("user_id").as("user_count"))
		df2pg(tpa_tp_brand_export, "tpa_tp_brand_data", data_model, regex_rule)

		// 11、生成触点品类
		val tp_brand_all_user = scene_tp_brand.groupBy("category", "tp").agg(countDistinct("user_id").as("brand_all_user"))

		val tp_category_user_count = tp_category.groupBy("category", "tp").agg(countDistinct("user_id").as("user_count"))
		val tp_category_tp_all_user = tp_category.groupBy("category").agg(countDistinct("user_id").as("tp_all_user"))

		val tpa_tp_category_export = tp_category_tp_all_user.join(tp_category_user_count, Seq("category"), "left_outer")
			.join(tp_brand_all_user, Seq("category", "tp"), "left_outer")
		df2pg(tpa_tp_category_export, "tpa_tp_category_data", data_model, regex_rule)

		/*************************************** 数据导出 *************************************************/

		scene_table.unpersist
		tp_scene_tp_filter.unpersist
		tp_people_angle_filter.unpersist
		scene_tp.unpersist()

	}

	/*＊
	* 将数据导入pg
	* */
	def df2pg(df: DataFrame, tablename: String, data_model: Int, regex_rule: String): Unit = {
		val columns = hiveContext.read.format("jdbc").jdbc(url, tablename, prop).columns
		df.na.fill(0)
			.withColumn("people_angle", lit(regex_rule))
			.withColumn("daterange", to_date(lit(daterange)))
			.withColumn("data_model", lit(data_model))
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
//			.setMaster("local[2]")
			.set("spark.default.parallelism", "635")

			.set("spark.sql.shuffle.partitions", "253") // 默认值 200
			.set("spark.sql.autoBroadcastJoinThreshold", "100") // 100M

//			.set("spark.shuffle.manager", "hash") // hash,sort,tungsten-sort
			.set("spark.shuffle.consolidateFiles", "true")


		if(args.length > 0) daterange = args(0)
		if(args.length > 1) mysql_host = args(1)

		sc = new SparkContext(conf)
		sc.setCheckpointDir("/tmp/spark/checkpoint")

		val tp_scene_point_attr = hiveContext.table("transforms.tp_scene_point_attr").repartition(857)
		val tp_people_angle = hiveContext.table("transforms.tp_people_angle")

		tp_scene_point_attr.persist(StorageLevel.MEMORY_AND_DISK_SER)
		tp_people_angle.persist(StorageLevel.MEMORY_AND_DISK_SER)

		val tpa_people_angle = hiveContext.read.format("jdbc").jdbc(url, "tpa_people_angle", prop) //.filter(expr("able"))
				.sort(expr("type_code").asc, expr("value").desc).collect()

		val data_func = get_count_data(tp_scene_point_attr, tp_people_angle, 0) _
		tpa_people_angle.foreach(row => data_func(row))

		tp_scene_point_attr.unpersist
		tp_people_angle.unpersist
		sc.stop()

		println(s"总共花费时间为： ${System.currentTimeMillis - strat}")
	}
}
