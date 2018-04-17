package com.marcpoint

import java.util.Properties

import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON


object DasTest_withcolumn {

	val url: String = "jdbc:mysql://172.16.1.100:3306/das"
	val prop: Properties = {
		val prop = new Properties()
		prop.put("user", "das")
		prop.put("password", "123")
		prop.put("driver", "com.mysql.jdbc.Driver")
		prop
	}
	// 设置应用初始化参数
	var tag_file = "media_brand_action_cut_baby_milk.xlsx"
	var input_table = "transforms.brand_counter"
	var output_table = "transforms.brand_action_cut_attr_new"
	var mode = "overwrite"
	var partition = 50
	var filter = "categoryid = 3 and brand_count > 1"

	// 配置参数
	val punctuation = "，|,|。|;|；|!|！|…"
	val opts = Map(
		"[&]" -> "and",
		"[|]" -> "or",
		"[-]" -> "and not",
		"(" ->"(",
		")" ->")"
	)

	val t2c_map = Map(
		"." -> "_dot_",
		"-" -> "_minus_",
		" " -> "_space_",
		"(" -> "left__",
		")" -> "__right",
		"&" -> "_at_",
		"|" -> "_or_"
	)
	val c2t_map = t2c_map.map(p => (p._2, p._1))

	// 环境参数
	var tags: Map[String, String] = null
	var dimensions: Map[String, String] = null
	var objs: Seq[String] = null
	var dataColumns: Array[String] = null

	var sc: SparkContext = null
	lazy val hiveContext: HiveContext = new HiveContext(sc)

	// 正则过滤方法
	def regexp_context(context: String): Seq[String] = {
		tags.keys.toSeq.map(x => s"${context} regexp concat('(?i)', ${x}) as ${x}")
	}

	// 将 列名中的特殊字符转换成 常规字符
	def t2c(col: String): String = {
		var c= col
		t2c_map.foreach(p => {c = c.replace(p._1, p._2)})
		c
	}

	// 将计算结果中的特殊字符进行反向转换
	def c2t(col: String): String = {
		var c = col.replaceFirst(",$", "")
		c2t_map.foreach(p => {c = c.replace(p._1, p._2)})
		c
	}



	//	def tagging_cut(taged_ck: DataFrame): DataFrame ={
	//		val tmp = objs.foldLeft(taged_ck)((df, object_name) => {
	//			val columns = tags.columns.filter(_.contains(object_name)).map(a => s"int(${a})")
	//			df.withColumn(object_name, expr(columns.mkString(" + ")) > 1)
	//		})
	//		tmp.show()
	//		if(tmp.filter(expr(objs.mkString(" or "))).takeAsList(1).isEmpty) return tmp
	//
	//		println(s"===============================切分短句并重打标签======================")
	//
	//		println(s"===============================测试开始======================")
	//		val df_cuted = tmp.filter(expr(objs.mkString(" or "))).selectExpr(dataColumns++objs:_*)
	//			.explode("content", "content_cute") {c : String=> c.split(punctuation)}
	//			.join(tags).selectExpr(regexp_context("content_cute")++dataColumns++objs:_*)
	//
	//		df_cuted.show()
	//		println(s"===============================测试完毕======================")
	//		tmp.filter(! expr(objs.mkString(" or "))).unionAll(df_cuted)
	//	}

	// 维度切割 分词
	def opsplit(exp: String): Array[String] = {
		var x = exp
		for((k, v) <- opts) {
			x = x.replace(k, s"\t${v}\t")
		}
		var opt_list = x.split("\t").filter(_.length != 0)

		for(i <- 0 until opt_list.length if ! opts.values.toArray.contains(opt_list(i))){
			opt_list(i) = t2c(opt_list(i))
		}
		opt_list
	}

	// #按维度打标签
	def wd_data(): List[(String, String)] = {
		dimensions.filterKeys(k => !tags.keys.toArray.contains(t2c(k))).map {
			p => {
				val opt_list = opsplit(p._2)
				for (i <- 0 until opt_list.length) {
					if (opt_list(i).contains("*")) {
						opt_list(i) = tags.keys.filter(_.startsWith(opt_list(i).replace("*", ""))).mkString(" or ")
					}
				}
				(p._1, opt_list.mkString(" "))
			}
		}.toList
	}

	// 解决 列过多 spark 处理报错问题
	def export_attrs(attr: Array[Column]): scala.collection.mutable.Map[String, Array[Column]] = {
		val attr_map = scala.collection.mutable.Map[String, Array[Column]]()
		for(i <- 0 to attr.length / 100) {
			attr_map += (s"attr${i}" -> attr.slice(i * 100, (i + 1) * 100))
		}
		attr_map
	}

	// 把切分过的短句 进行合并
	def cut_combine(wd_df: DataFrame): DataFrame = {
		val sum_dim = dimensions.keys.toSeq.map(p => max(p).as(p))
		wd_df.groupBy(dataColumns.head, dataColumns.tail:_*).agg(sum_dim.head, sum_dim.tail:_*)
	}

	// 数据导出保存
	def exprot(wd_df1: DataFrame): Unit = {
		import hiveContext.implicits._

		// 如果有对象则先把切短过短句的合并
		val wd_df = if(objs.length == 0) wd_df1 else cut_combine(wd_df1)
		wd_df.cache()
		wd_df.show()

		val attr_data = dimensions.keys.map(k => s"if(${k} = true, '${c2t(k)},', '') as ${k}")
		val c_attrs = dimensions.keys.toArray.map(x => expr(x))

		val attr_map = export_attrs(c_attrs)
		val attrs = attr_map.map(p => concat(p._2:_*).as(p._1)).toArray
		val c_dataColumns = dataColumns.map(expr(_)).toSeq

		val attrs_base = wd_df.selectExpr(dataColumns++attr_data:_*).select(c_dataColumns++attrs:_*)
		attrs_base.cache()
		attrs_base.show()

		val attr = concat(attr_map.keys.toArray.map(expr(_)):_*).as("attr")
		var output_df = attrs_base.select(c_dataColumns:+attr:_*).filter($"attr" !== "")
			.explode("attr", "attr_new") {s: String => s.split(",")}.drop("attr").withColumnRenamed("attr_new", "attr")

		println(s"===============================数据总大小为：${output_df.count()}")
		output_df.show()
		output_df.write.format("orc").mode(mode).saveAsTable(output_table)

		wd_df.unpersist()
		attrs_base.unpersist()

	}

	// 初始化词库信息
	def init_tag(): Unit = {
		import hiveContext.implicits._
		val das_rules = hiveContext.read.jdbc(url, "rules", prop).filter($"name" === tag_file).first()

		tags = JSON.parseFull(das_rules.getString(2)).get.asInstanceOf[Map[String, String]].map(p =>(t2c(p._1), p._2))

//		val tags_source = hiveContext.read.json(sc.makeRDD(das_rules.getString(2).stripMargin::Nil))
//		tags = tags_source.columns.foldLeft(tags_source)((acc, ca) => acc.withColumnRenamed(ca, t2c(ca)))

		dimensions = JSON.parseFull(das_rules.getString(3)).getOrElse(Map()).asInstanceOf[Map[String, String]].map(p =>(t2c(p._1), p._2))
		objs = JSON.parseFull(das_rules.getString(4)).getOrElse(Nil).asInstanceOf[Seq[String]].map(t2c)

	}

	/*
	// 切分短句并重打标签
	def tagging_cut(taged_ck: DataFrame): DataFrame ={
		val tmp = objs.foldLeft(taged_ck)((df, object_name) => {
			val columns = tags.keys.filter(_.contains(object_name)).map(a => s"int(${a})")
			df.withColumn(object_name, expr(columns.mkString(" + ")) > 1)
		})
		tmp.cache()

		val tmp_obj = tmp.filter(expr(objs.mkString(" or "))).selectExpr(dataColumns++objs:_*)
		tmp_obj.cache()
		tmp_obj.show()

		if(tmp_obj.takeAsList(1).isEmpty) return tmp

		println(s"===============================切分短句并重打标签======================")

		println(s"===============================测试开始======================")

		var df_cuted_t2 = tmp_obj.join(broadcast(tags)).repartition(partition, expr("id"))
		df_cuted_t2.cache()
		df_cuted_t2.show()

		println(s"=============================== cross join 成功 ======================")

		val df_cuted_t3 = df_cuted_t2.explode("content", "content_cute") {c : String=> c.split(punctuation)}.repartition(partition)
		df_cuted_t3.show()
		println(s"============================= explode 成功 ========================")

		val df_cuted = df_cuted_t2.selectExpr(regexp_context("content_cute")++dataColumns++objs:_*)
		df_cuted.show()

		println(s"===============================测试完毕======================")

		val cut_taged = tmp.filter(! expr(objs.mkString(" or "))).unionAll(df_cuted)
		tmp.unpersist()
		tmp_obj.unpersist()
		cut_taged
	}

	*/


	// 批量添加列
	def combine_tag(df_data: DataFrame, columns_map: Map[String, String]): DataFrame = {
		val new_df = columns_map.foldLeft(df_data)((df, p) => {
			df.withColumn(p._1, lit(p._2))
		})
		new_df
	}

	/*＊
	* 主方法
	*
	* */
	def main(args: Array[String]): Unit = {

		val strat: Long = System.currentTimeMillis
		if(args.length > 2) {
			tag_file = args(0)
			input_table = args(1)
			output_table = args(2)
			if(args.length > 3) mode = args(3)
			if(args.length > 4) partition = args(4).toInt
			if(args.length > 5) filter = args(5)
		}

		val conf = new SparkConf()
//			.setMaster("local[8]")
			.setAppName("Read File")
			.set("spark.default.parallelism", "237")
			.set("spark.sql.crossJoin.enabled", "true")
			.set("spark.sql.autoBroadcastJoinThreshold", "1000000000")

		sc = new SparkContext(conf)


		// 环境初始化
		val media_choice_post = hiveContext.table(input_table).filter(filter)
//		val brand_counter_buct = hiveContext.table("transforms.brand_counter_buct")
//		val brand_counter_no_buct = hiveContext.table("transforms.brand_counter_no_buct")

		dataColumns = media_choice_post.columns
		init_tag()

		val taged_ck = combine_tag(media_choice_post, tags).selectExpr(regexp_context("content")++dataColumns:_*)

		taged_ck.cache()
		taged_ck.show()

		sc.stop()

		println(s"总共花费时间为： ${System.currentTimeMillis - strat}")
	}
}
