package com.marcpoint

import java.sql.{Connection, DriverManager}
import java.util.Properties

import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}


object TouchPointNewNew {

	var daterange: String = "2018-01-15"
	var mysql_host: String =  "172.16.1.100"
	var month_update: String = "0" // 是否是月度更新 0: 不是， 1: 是

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

	//	def arl_fun = (a: String, b: String, c: Int) => if(a == null) null else a.split(b)(c)

	def arl_fun (a: String, b: String, c: Int): String = {
		if (a == null) return null
		val arr = a.split(b)
		if (c < 0)
			arr(arr.length + c)
		else
			arr(c)
	}

	def arl = udf(arl_fun _)


	/*
	* 计算指标
	*
	* */
	def get_count_data(tp_scene_point_attr: DataFrame, tp_people_angle: DataFrame, data_model: Long): Unit ={
		import hiveContext.implicits._

		/*************************************** 初始化提取数据 *************************************************/

		val people_angle_filter = tp_scene_point_attr.join(tp_people_angle, Seq("platform_id", "user_id"), "inner")
			.explode("regex_rule", "people_angle") { r: Seq[String] => r}
			.select($"people_type", $"people_angle", $"id", $"user_id", $"attr", $"attr_type", $"platform_id", $"categoryids", $"brands")
			.dropDuplicates().repartition(997)
		people_angle_filter.cache()



		val attr_column = Seq($"people_type", $"people_angle", $"id", $"user_id", $"platform_id", $"categoryids", $"brands", $"attr_type")
		val scene_table = people_angle_filter.filter($"attr_type" === "scene").select(attr_column:+ $"attr".as("scene"):_*).alias("a")
		val tp_table = people_angle_filter.filter($"attr_type".isin("tp", "sub_tp")).select(attr_column:+ $"attr".as("tp"):_*).alias("b")

		scene_table.cache()
		tp_table.cache()

		val tp_scene_tp_filter = scene_table.join(tp_table, $"a.people_type" === $"b.people_type" && $"a.people_angle" === $"b.people_angle" && $"a.platform_id" === $"b.platform_id" && $"a.id" === $"b.id", "outer")
			.select(
				iff($"b.id".isNull, $"a.people_type", $"b.people_type").as("people_type")
				, iff($"b.id".isNull, $"a.people_angle", $"b.people_angle").as("people_angle")
				, iff($"b.id".isNull, $"a.id", $"b.id").as("id")
				, iff($"b.id".isNull, $"a.user_id", $"b.user_id").as("user_id")
				, $"a.scene"
				, iff($"b.id".isNull, $"a.platform_id", $"b.platform_id").as("platform_id")
				, ifa($"b.id".isNull, $"a.categoryids", $"b.categoryids").as("categoryids")
				, ifa($"b.id".isNull, $"a.brands", $"b.brands").as("brands")
				, $"b.tp"
				, $"b.attr_type"
			).repartition(997)

		tp_scene_tp_filter.cache()

		/*************************************** 计算场景触点 *************************************************/

		// 1、生成场景数据
		val inside_track_table = people_angle_filter.filter($"attr_type".isin("scene_A", "scene_B")).alias("c")
		val scene_inside_track_table = scene_table.join(inside_track_table, $"a.people_type" === $"c.people_type" && $"a.people_angle" === $"c.people_angle" && $"a.platform_id" === $"c.platform_id" && $"a.id" === $"c.id", "inner")
			.groupBy($"a.people_type", $"a.people_angle", $"a.scene").agg(
			countDistinct($"a.user_id").as("inside_track_user")
			, countDistinct(when($"c.attr_type" === "scene_A", $"a.user_id").otherwise(null)).as("inside_track_A_user")
			, countDistinct(when($"c.attr_type" === "scene_B", $"a.user_id").otherwise(null)).as("inside_track_B_user")
		)

		val scene_user_count = tp_scene_tp_filter.filter($"scene".isNotNull).groupBy($"people_type", $"people_angle", $"scene")
			.agg(
				countDistinct("user_id").as("user_count")
				, countDistinct(when($"tp".isNotNull, $"user_id").otherwise(null)).as("tp_user")
			)
		val all_scene_user = scene_table.groupBy($"people_type", $"people_angle").agg(countDistinct("user_id").as("all_scene_user"))

		val tpa_scene_export = scene_user_count.join(scene_inside_track_table, Seq("people_type", "people_angle", "scene"), "left_outer")
			.join(all_scene_user, Seq("people_type", "people_angle"), "left_outer")

		df2pg(tpa_scene_export, "tpa_scene_data", data_model)

		// 2. 生成场景品类数据
		val c_scene_table = scene_table.filter($"categoryids".isNotNull).explode("categoryids", "category") {c: Seq[String] => c }.alias("d")
		val c_scene_inside_track_table = c_scene_table.join(inside_track_table, $"d.people_type" === $"c.people_type" && $"d.people_angle" === $"c.people_angle" && $"d.platform_id" === $"c.platform_id" && $"d.id" === $"c.id", "inner")
			.groupBy($"d.people_type", $"d.people_angle", $"d.scene").agg(
			countDistinct($"d.user_id").as("inside_track_user")
			, countDistinct(when($"c.attr_type" === "scene_A", $"d.user_id").otherwise(null)).as("inside_track_A_user")
			, countDistinct(when($"c.attr_type" === "scene_B", $"d.user_id").otherwise(null)).as("inside_track_B_user")
		)

		val c_scene_user_count = tp_scene_tp_filter.filter($"scene".isNotNull && $"categoryids".isNotNull)
			.explode("categoryids", "category") {c: Seq[String] => c }
			.groupBy($"people_type", $"people_angle", $"category", $"scene")
			.agg(
				countDistinct("user_id").as("user_count")
				, countDistinct(when($"tp".isNotNull, $"user_id").otherwise(null)).as("tp_user")
			)

		val c_all_scene_user = scene_table.filter($"categoryids".isNotNull)
			.explode("categoryids", "category") {c: Seq[String] => c }.groupBy($"people_type", $"people_angle", $"category")
			.agg(countDistinct("user_id").as("all_scene_user"))

		val tpa_scene_category_export = c_scene_user_count.join(c_scene_inside_track_table, Seq("people_type", "people_angle", "category", "scene"), "left_outer")
			.join(c_all_scene_user, Seq("people_type", "people_angle", "category"), "left_outer")
			.select($"people_type", $"people_angle", $"category".cast(IntegerType), $"scene", $"user_count"
				, $"inside_track_A_user", $"inside_track_B_user", $"inside_track_user"
				, $"tp_user", $"all_scene_user"
			)

		df2pg(tpa_scene_category_export, "tpa_scene_category_data", data_model)

		// 3. 生成场景品牌数据
		val b_scene_table = scene_table.filter($"brands".isNotNull).explode("brands", "brand") {c: Seq[String] => c }
			.select($"people_type", $"people_angle", $"user_id", $"scene", split($"brand", "_")(0).cast(IntegerType).as("category"), split($"brand", "_")(1).cast(IntegerType).as("brand"))
		b_scene_table.cache()

		val t1 = b_scene_table.groupBy("people_type", "people_angle", "scene", "category", "brand").agg(countDistinct("user_id").as("user_count"))
		val t2 = b_scene_table.groupBy("people_type", "people_angle", "scene", "category").agg(countDistinct("user_id").as("brand_all_user"))
		val tpa_scene_brand_export = t1.join(t2, Seq("people_type", "people_angle", "scene", "category"), "left_outer")

		df2pg(tpa_scene_brand_export, "tpa_scene_brand_data", data_model)
		b_scene_table.unpersist()

		// 4. 生成子场景数据
		val tpa_subscene_export = people_angle_filter.filter($"attr_type" === "sub_scene").groupBy("people_type", "people_angle", "attr")
			.agg(countDistinct("user_id").as("user_count"), countDistinct("id").as("id_count"))
			.select($"people_type", $"people_angle", split($"attr", "\\.")(1).as("parent_scene"), split($"attr", "\\.")(2).as("scene"), $"user_count", $"id_count")

		df2pg(tpa_subscene_export, "tpa_subscene_data", data_model)

		// 5. 生成子场景品类数据
		val tpa_subscene_category_export = people_angle_filter.filter($"attr_type" === "sub_scene" && $"categoryids".isNotNull)
			.explode("categoryids", "category") {c: Seq[String] => c }
			.groupBy("people_type", "people_angle", "category", "attr").agg(countDistinct("user_id").as("user_count"), countDistinct("id").as("id_count"))
			.select($"people_type", $"people_angle", split($"attr", "\\.")(1).as("parent_scene"), split($"attr", "\\.")(2).as("scene"), $"user_count", $"id_count", $"category".cast(IntegerType))

		df2pg(tpa_subscene_category_export, "tpa_subscene_category_data", data_model)


		// 6. 生成场景触点数据
		val tpa_scene_tp_export = tp_scene_tp_filter.filter($"scene".isNotNull && $"tp".isNotNull).select(
			$"people_type", $"people_angle", $"scene", $"user_id"
			, when($"attr_type" === "sub_tp", split($"tp", "\\.")(1)).otherwise("self").as("parent_tp")
			, when($"attr_type" === "sub_tp", split($"tp", "\\.")(2)).otherwise($"tp").as("tp")
		).groupBy("people_type", "people_angle", "scene", "parent_tp", "tp").agg(countDistinct("user_id").as("user_count"))

		df2pg(tpa_scene_tp_export, "tpa_scene_tp_data", data_model)

		// 7. 生成场景触点品类的数据
		val s1 = tp_scene_tp_filter.filter($"scene".isNotNull && $"tp".isNotNull && $"categoryids".isNotNull)
			.select($"people_type", $"people_angle", $"scene", $"user_id", $"categoryids"
				, when($"attr_type" === "sub_tp", split($"tp", "\\.")(1)).otherwise("self").as("parent_tp")
				, when($"attr_type" === "sub_tp", split($"tp", "\\.")(2)).otherwise($"tp").as("tp")
			).explode("categoryids", "category") {c: Seq[String] => c }

		s1.cache()
		val base_count = s1.groupBy("people_type", "people_angle", "scene", "parent_tp", "tp", "category").agg(countDistinct("user_id").as("user_count"))
		val tp_count = s1.groupBy("people_type", "people_angle", "scene", "category").agg(countDistinct("user_id").as("tp_user_count"))
		val scene_count = s1.groupBy("people_type", "people_angle", "parent_tp", "tp", "category").agg(countDistinct("user_id").as("scene_user_count"))
		val tpa_scene_tp_category_export = base_count.join(tp_count, Seq("people_type", "people_angle", "scene", "category"), "left_outer")
			.join(scene_count, Seq("people_type", "people_angle", "parent_tp", "tp", "category"), "left_outer")
			.select($"people_type", $"people_angle", $"category".cast(IntegerType), $"scene", $"parent_tp", $"tp", $"user_count", $"tp_user_count", $"scene_user_count")

		df2pg(tpa_scene_tp_category_export, "tpa_scene_tp_category_data", data_model)
		s1.unpersist()

		// 8. 生成场景触点品牌数据
		val e1 = tp_scene_tp_filter.filter($"scene".isNotNull && $"tp".isNotNull && $"brands".isNotNull && $"attr_type" === "tp")
			.explode("brands", "brand") {c: Seq[String] => c }
			.select($"people_type", $"people_angle", $"user_id", $"scene", $"tp", split($"brand", "_")(0).cast(IntegerType).as("category"), split($"brand", "_")(1).cast(IntegerType).as("brand"))
		e1.cache()
		val scene_tp_brand_user_count = e1.groupBy("people_type", "people_angle", "scene", "tp", "category", "brand").agg(countDistinct("user_id").as("user_count"))
		val scene_tp_category_user_count = e1.groupBy("people_type", "people_angle", "scene", "tp", "category").agg(countDistinct("user_id").as("brand_all_user"))

		val tpa_scene_tp_brand_export = scene_tp_brand_user_count.join(scene_tp_category_user_count, Seq("people_type", "people_angle", "scene", "tp", "category"), "left_outer")
		df2pg(tpa_scene_tp_brand_export, "tpa_scene_tp_brand_data", data_model)
		e1.unpersist()

		// 9. 生成触点数据
		val tp_scene_user_count = tp_scene_tp_filter.filter($"tp".isNotNull && $"attr_type" === "tp")
			.groupBy("people_type", "people_angle", "tp").agg(
			countDistinct(when($"scene".isNotNull, $"user_id").otherwise(null)).as("scene_user")
			,countDistinct("user_id").as("user_count")
		)
		val all_tp_user_count = people_angle_filter.filter($"attr_type" === "tp").groupBy("people_type", "people_angle")
			.agg(countDistinct("user_id").as("tp_all_user"))

		val tpa_tp_export = tp_scene_user_count.join(all_tp_user_count, Seq("people_type", "people_angle"), "left_outer")
		df2pg(tpa_tp_export, "tpa_tp_data", data_model)

		people_angle_filter.unpersist()

		// 10. 生成触点品牌
		val tpa_tp_brand_export = tp_table.filter($"attr_type" === "tp" && $"brands".isNotNull).explode("brands", "brand") {c: Seq[String] => c }
			.select($"people_type", $"people_angle", $"user_id", $"tp"
				, split($"brand", "_")(0).cast(IntegerType).as("category"), split($"brand", "_")(1).cast(IntegerType).as("brand"))
			.groupBy("people_type", "people_angle", "tp", "category", "brand").agg(countDistinct("user_id").as("user_count"))

		df2pg(tpa_tp_brand_export, "tpa_tp_brand_data", data_model)

		// 11. 生成触点品类
		val tp_user_count = tp_table.filter($"attr_type" === "tp" && $"brands".isNotNull).explode("brands", "brand") {c: Seq[String] => c }
			.select($"people_type", $"people_angle", $"tp", split($"brand", "_")(0).cast(IntegerType).as("category"), $"user_id")
			.groupBy("people_type", "people_angle", "category", "tp").agg(countDistinct("user_id").as("brand_all_user"))

		val tp_category = tp_table.filter($"attr_type" === "tp" && $"categoryids".isNotNull).explode("categoryids", "category") {c: Seq[String] => c }
		val user_count = tp_category.groupBy("people_type", "people_angle", "category", "tp").agg(countDistinct("user_id").as("user_count"))
		val tp_all_user = tp_category.groupBy("people_type", "people_angle", "category").agg(countDistinct("user_id").as("tp_all_user"))

		val tpa_tp_category_export = tp_user_count.join(user_count, Seq("people_type", "people_angle", "category", "tp"), "left_outer")
			.join(tp_all_user, Seq("people_type", "people_angle", "category"), "left_outer")
			.select($"people_type", $"people_angle", $"category".cast(IntegerType), $"tp", $"brand_all_user", $"user_count", $"tp_all_user")
		df2pg(tpa_tp_category_export, "tpa_tp_category_data", data_model)


		scene_table.unpersist()
		tp_table.unpersist()
		people_angle_filter.unpersist()
		tp_scene_tp_filter.unpersist()
	}

	/**
		* 将数据导入pg
		* @param df
		* @param tablename
		*/
	def df2pg(df: DataFrame, tablename: String, data_model: Long): Unit = {
		val columns = hiveContext.read.format("jdbc").jdbc(url, tablename, prop).columns
		df.na.fill(0)
			.withColumn("daterange", to_date(lit(daterange)))
			.withColumn("data_model", lit(data_model))
			.selectExpr(columns: _*).coalesce(53).write.mode(SaveMode.Append).jdbc(url, tablename, prop)
	}

	/**
		* 更新 数据状态
		* @param sql
		*/
	def update_mysql(sql: String, isQ: Boolean): Unit = {
		var connection: Connection = null
		try {
			Class.forName(prop.getProperty("driver"))
			connection = DriverManager.getConnection(url, prop.getProperty("user"), prop.getProperty("password"))
			val statement = connection.createStatement
			if (isQ) statement.executeQuery(sql) else statement.executeUpdate(sql)
		} catch {
			case e: Exception => throw e
		} finally {
			if (null != connection) connection.close
		}
	}

	/**
		* 主方法
		* memoryOverhead = sparkConf.getInt("spark.yarn.executor.memoryOverhead", math.max((MEMORY_OVERHEAD_FACTOR * executorMemory).toInt, MEMORY_OVERHEAD_MIN))
		* @param args
		*/
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
		if(args.length > 1) month_update = args(1) // 是否是月度更新 0: 不是， 1: 是
		if(args.length > 2) mysql_host = args(2)

		sc = new SparkContext(conf)
		import hiveContext.implicits._
		sc.setCheckpointDir("/tmp/spark/checkpoint")
		hiveContext.udf.register("arl", arl_fun _)

		val tp_scene_point_attr = hiveContext.table("transforms.tp_scene_point_attr").repartition(857)
		val tp_people_angle = hiveContext.table("transforms.tp_people_angle")
		tp_people_angle.cache()

		if(month_update == "1") {
			println(s"重置数据模型， 开始月度数据更新!")
			update_mysql("select tpa_month_data_update()", true)
		}

		val special_people = hiveContext.read.format("jdbc").jdbc(url, "tpa_special_people", prop).filter($"state".isin(2, 4))

		special_people.collect().foreach(row => {
			val sp_list = row.getAs[String]("value").split(",")
			val id = row.getAs[Long]("id")

			println(s"update state of 0 model to begin: update tpa_special_people set state = 1 where id = ${id} ")
			update_mysql(s"update tpa_special_people set state = 1 where id = ${id} ", false)

			val cs = Seq("id", "user_id", "attr_type", "categoryids", "brands", "platform_id")
			var base_data = tp_scene_point_attr.filter((lit("").isin(sp_list:_*) || (! $"attr".like("场景.%")) || arl($"attr", lit("\\."), lit(-1)).isin(sp_list:_*)) && size(split($"attr", "\\.")).isin(2, 3))
				.select($"id", $"user_id", $"attr",
					when($"attr".like("TP.%") && size(split($"attr", "\\.")) === 3, "sub_tp")
						.when($"attr".like("TP.%") && size(split($"attr", "\\.")) === 2, "tp")
						.when($"attr".like("%.A.%"), "scene_A")
						.when($"attr".like("%.B.%"), "scene_B")
						.when($"attr".like("场景.%"), "sub_scene,scene").as("attr_types"),
					$"categoryids", $"brands", $"platform_id")
				.explode("attr_types", "attr_type") {c: String => c.split(",")}
				.selectExpr(cs:+"if(attr_type in ('tp', 'scene'), arl(attr, '\\.', 1), attr) as attr":_*).dropDuplicates(cs:+"attr")
			//				.write.format("orc").mode("overwrite").saveAsTable("extract.tpa_test")

			get_count_data(base_data, tp_people_angle, id)

			// 更新 可用标示
			println(s"update state of 0 model to secuss: update tpa_special_people set state = 0 where id = ${id} ")
			update_mysql(s"update tpa_special_people set state = 0 where id = ${id} ", false)

		})

		tp_people_angle.unpersist
		sc.stop()

		println(s"总共花费时间为： ${System.currentTimeMillis - strat}")
	}


}
