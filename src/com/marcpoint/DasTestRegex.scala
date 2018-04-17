package com.marcpoint

import java.util.Properties

import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType, BooleanType}

import scala.util.parsing.json.JSON
import java.util.regex.Pattern


object DasTestRegex {

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
	var output_table = "transforms.brand_action_cut_attr_new1"
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
		"|" -> "_or_",
		"/" -> "_bs_"
	)
	val c2t_map = t2c_map.map(p => (p._2, p._1))

	// 环境参数
	var tags: Seq[(String, Pattern)] = _
	var dimensions: Map[String, String] = _
	var objs: Seq[String] = _
	var dataColumns: Array[String] = _

	var sc: SparkContext = _
	lazy val hiveContext: HiveContext = new HiveContext(sc)

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

	// 初始化词库信息
	def init_tag(): Unit = {
		import hiveContext.implicits._
		val das_rules = hiveContext.read.jdbc(url, "rules", prop).filter($"name" === tag_file).first()

		tags = JSON.parseFull(das_rules.getString(2)).get.asInstanceOf[Map[String, String]]
			.toSeq.map(p =>(t2c(p._1), Pattern.compile(p._2, Pattern.CASE_INSENSITIVE)))

		dimensions = JSON.parseFull(das_rules.getString(3)).getOrElse(Map()).asInstanceOf[Map[String, String]].map(p =>(t2c(p._1), p._2))
		objs = JSON.parseFull(das_rules.getString(4)).getOrElse(Nil).asInstanceOf[Seq[String]].map(t2c)
	}

	// 切分短句并重打标签
	def tagging_cut(taged_ck: DataFrame): DataFrame ={

		val tmp = objs.foldLeft(taged_ck)((df, object_name) => {
			val columns = tags.map(_._1).filter(_.contains(object_name)).map(a => s"int(${a})")
			df.withColumn(object_name, expr(columns.mkString(" + ")) > 1)
		})

		val need_cut_df = tmp.filter(expr(objs.mkString(" or ")))
		if(need_cut_df.takeAsList(1).isEmpty) return tmp

		println(s"=============================== 切分对象打标签 ======================")
		val explode_df = need_cut_df.selectExpr(dataColumns++objs:_*)
			.explode("content", "content_cute") {c : String=> c.split(punctuation)}
		val need_cuted_df = combine_tag(explode_df, "content_cute").drop("content_cute").selectExpr(taged_ck.columns++objs:_*)

		tmp.filter(! expr(objs.mkString(" or "))).unionAll(need_cuted_df)
	}

	// #按维度打标签
	def wd_data(): List[(String, String)] = {
		dimensions.filterKeys(k => !tags.map(_._1).contains(t2c(k))).map {
			p => {
				val opt_list = opsplit(p._2)
				for (i <- 0 until opt_list.length) {
					if (opt_list(i).contains("*")) {
						opt_list(i) = tags.map(_._1).filter(_.startsWith(opt_list(i).replace("*", ""))).mkString(" or ")
					}
				}
				(p._1, opt_list.mkString(" "))
			}
		}.toList
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

		// 合并 attr
		val dim_keys = dimensions.keys.toSeq
		val param_dict_bc = sc.broadcast((dim_keys.map(c2t), dataColumns.length, 0 until dim_keys.length))

		val rdd = wd_df.selectExpr(dataColumns++dim_keys:_*).map {
			case row => {
				val param_dict = param_dict_bc.value
				val rowSeq = row.toSeq
				val dim_row = rowSeq.takeRight(param_dict._1.length)

				val attr = param_dict._3.map(i => if(dim_row(i).asInstanceOf[Boolean]) s"${param_dict._1(i)}," else "").mkString
				Row.fromSeq(rowSeq.take(param_dict._2):+attr)
			}
		}

		val schema = StructType(wd_df.schema.fields.take(dataColumns.length):+StructField("attr", StringType))
		val output_df = hiveContext.createDataFrame(rdd, schema).filter($"attr" !== "")
			.explode("attr", "attr_new") {s: String => s.split(",")}.drop("attr").withColumnRenamed("attr_new", "attr")

		output_df.write.format("orc").mode(mode).saveAsTable(output_table)
	}

	// 批量添加列
	def combine_tag(df_data: DataFrame, content_col: String): DataFrame = {

		val index = sc.broadcast(df_data.columns.indexOf(content_col))
		val tags_bc = sc.broadcast(tags)

		val rdd = df_data.map {
			case row => {
				val rs = row.toSeq
				Row.fromSeq(rs++tags_bc.value.map(_._2.matcher(rs(index.value).asInstanceOf[String]).find))
			}
		}
		val schema = StructType(df_data.schema.fields++tags.map(p => StructField(p._1, BooleanType)))
		hiveContext.createDataFrame(rdd, schema)
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
			.set("spark.sql.autoBroadcastJoinThreshold", "1000000000")

		sc = new SparkContext(conf)
		// 环境初始化
		val media_choice_post = hiveContext.table(input_table).filter(filter).repartition(partition, expr("id"))
		dataColumns = media_choice_post.columns
		init_tag()

		// 按叶子词库打标签
		println(s"===============================按叶子词库打标签======================")
		val taged_ck = combine_tag(media_choice_post, "content")

		// 切分短句并重打标签
		val taged_cut = if(objs.length == 0) taged_ck else tagging_cut(taged_ck)

		// 按维度打标签
		println(s"===============================按维度打标签======================")
		val wd_column = wd_data().map(p => s"${p._2} as ${p._1}")
		val wd_df = taged_cut.selectExpr(taged_cut.columns++wd_column:_*)

		// 输出
		println(s"===============================输出======================")
		exprot(wd_df)

		sc.stop()

		println(s"总共花费时间为： ${System.currentTimeMillis - strat}")
	}
}
