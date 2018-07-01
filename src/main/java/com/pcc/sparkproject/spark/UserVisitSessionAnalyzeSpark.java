package com.pcc.sparkproject.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

import com.alibaba.fastjson.JSONObject;
import com.pcc.sparkproject.conf.ConfigrationManager;
import com.pcc.sparkproject.constant.Constant;
import com.pcc.sparkproject.dao.ITaskDao;
import com.pcc.sparkproject.dao.impl.DaoFactory;
import com.pcc.sparkproject.domian.Task;
import com.pcc.sparkproject.test.MockData;
import com.pcc.sparkproject.util.ParamUtils;
import com.pcc.sparkproject.util.StringUtils;

import scala.Tuple2;

/**
 * 用户访问session分析的spark任务
 * 
 * @author 99653
 *
 */
public class UserVisitSessionAnalyzeSpark {
	public static void main(String[] args) {
		// 得到spark上下文
		SparkConf conf = new SparkConf().setAppName(Constant.SPARK_APP_NAME_SESSION).setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = getSQLContext(sc);
		// 数据层的组件
		ITaskDao taskDao = DaoFactory.getTaskDao();
		// 是否模拟数据
		mock(sc, sqlContext);
		// 根据传递的参数拿到taskid
		long taskid = ParamUtils.getTaskIdFromArgs(args);
		// 取数据到DAO
		Task task = taskDao.findById(taskid);
		// 得到人物参数的JSON对象
		JSONObject taskPrama = JSONObject.parseObject(task.getTaskParam());
		// 根据时间参数拿到用户行为表的RDD
		JavaRDD<Row> actionRDD = getActionRDDByDateRange(sqlContext, taskPrama);

		/**
		 * 下面要将用户行为数据和用户信息数据的两表进行聚合 两表通过user_id关联 最后的结果是：session_id ---- 多种行为 + 用户信息
		 * 一个session又多种行为，所以先要将一个session做聚合，把多种行为聚合在一起 再将session对应的用户信息关联上
		 * 
		 */
		//得到聚合后的RDD
		JavaPairRDD<String, String> sessionid2ActionInfoRDD = aggregateBySession(sqlContext, actionRDD);
		
		// 关闭spark上下文
		sc.close();

	}

	/**
	 * 是否需要生成本地数据进行调试
	 * 
	 * @param sc
	 * @param sqlContext
	 */
	private static void mock(JavaSparkContext sc, SQLContext sqlContext) {
		if (ConfigrationManager.getBoolean(Constant.SPARK_LOCAL)) {
			// 本地
			MockData.mock(sc, sqlContext);
		}
	}

	/**
	 * 2.0以后SQLCONTEXT HIVECONTEXT SPARKCONTEXT合并在SPARKSESION 分本地和生产环境 获取SQLContext
	 * 
	 * @return
	 */
	public static SQLContext getSQLContext(JavaSparkContext sc) {
		if (ConfigrationManager.getBoolean(Constant.SPARK_LOCAL)) {
			return new SQLContext(sc);
		} else {
			return new HiveContext(sc);
		}
	}

	/**
	 * 通过数据库查询出来的params，来获取时间参数 并且得到指定时间范围用户访问行为数据
	 * 
	 * @param sqlContext
	 * @param paramJS
	 * @return
	 */
	public static JavaRDD<Row> getActionRDDByDateRange(SQLContext sqlContext, JSONObject paramJS) {

		// 拿到时间范围
		String startDate = ParamUtils.getParam(paramJS, Constant.PARAM_START_DATE);
		String endDate = ParamUtils.getParam(paramJS, Constant.PARAM_END_DATE);
		// 建立SQL语句
		String sql = "select * " + "from user_visit_action" + "where date between '" + startDate + "' and '" + endDate
				+ "'";

		// 执行
		Dataset<Row> datasets = sqlContext.sql(sql);
		return datasets.toJavaRDD();
	}

	/**
	 * 获取所有用户信息的RDD
	 * 
	 * @param sqlContext
	 * @param paramJS
	 * @return
	 */
	public static JavaRDD<Row> getUserInfoRDD(SQLContext sqlContext) {
		String sql = "select * from user_info";
		// 执行
		Dataset<Row> datasets = sqlContext.sql(sql);
		return datasets.toJavaRDD();
	}

	/**
	 * 按照session粒度进行聚合
	 * 
	 * @param actionRDD
	 *            用户行为的元素数据RDD
	 * @return
	 */
	public static JavaPairRDD<String, String> aggregateBySession(SQLContext sqlContext, JavaRDD<Row> actionRDD) {
		// Row相当于数据库表中的一行数据

		// 转换成PairRDD key - value
		// mapToPair对每个Row通过函数转换成Tuple<KEY,VALUE>
		JavaPairRDD<String, Row> sessionidPairRDD = actionRDD.mapToPair((row) -> {
			return new Tuple2<String, Row>(row.getString(2), row);
		});

		// 用sessionid分组
		JavaPairRDD<String, Iterable<Row>> sessionid2rowRDD = sessionidPairRDD.groupByKey();

		// 对每个sessionid的分组进行聚合，主要是聚合每个action的点击品类和搜索关键词
		// 对pairRDD进行map是处理tuple
		// 返回<usrid,string>
		JavaPairRDD<Long, String> userid2partActionRDD = sessionid2rowRDD.mapToPair((tuple) -> {
			StringBuffer sSearchWordBuffer = new StringBuffer("");
			StringBuffer sClickCategoryIdsBuffer = new StringBuffer("");
			String sessionid = tuple._1;
			Long userid = null;

			// 遍历每一种行为，一种行为对应一个Row
			for (Row row : tuple._2) {
				// 获取该session的userid
				if (userid != null) {
					userid = row.getLong(1);
				}
				String searchKeyWord = row.getString(5);
				Long clickCategoryId = row.getLong(6);
				// 表的格式问题
				// 搜索行为才有searchKeyWord字段
				// 点击品类行为才有clickCategoryId字段
				// 要判断一下
				if (StringUtils.isNotEmpty(searchKeyWord)) {
					if (!sSearchWordBuffer.toString().contains(sSearchWordBuffer)) {
						sSearchWordBuffer.append(searchKeyWord + ",");
					}
				}
				if (clickCategoryId != null) {
					if (!sClickCategoryIdsBuffer.toString().contains(clickCategoryId.toString())) {
						sClickCategoryIdsBuffer.append(clickCategoryId + ",");
					}
				}
			}

			String searchKeyWords = StringUtils.trimComma(sSearchWordBuffer.toString());
			String clickCategoryIds = StringUtils.trimComma(sClickCategoryIdsBuffer.toString());

			// 这里完成后下一步需要和userInfo链接，为了方便这里可以把key改成userid
			// <userid,row>-+-+<userid,string> --> <sessionid,string>
			// string的数据格式为 key=value|key=value
			String partAggInfo = Constant.FIELD_SESSION_ID + "=" + sessionid + "|" + Constant.FIELD_SEARCH_KEYWORDS
					+ "=" + searchKeyWords + "|" + Constant.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds;

			// 返回<userid,string (sessionid=?|searchkeywords=?|..)>
			return new Tuple2<Long, String>(userid, partAggInfo);
		});

		// 得到userInfoRDD
		JavaRDD<Row> userInfoRDD = getUserInfoRDD(sqlContext);
		// 转化为key-value
		JavaPairRDD<Long, Row> userid2InfoRDD = userInfoRDD.mapToPair((row) -> {
			return new Tuple2<Long, Row>(row.getLong(0), row);
		});

		// 通过key进行拼接
		JavaPairRDD<Long, Tuple2<String, Row>> userid2FullInfoRDD = userid2partActionRDD.join(userid2InfoRDD);
		// 处理拼接后的数据
		JavaPairRDD<String, String> sessionid2FullAggrInfoRDD = userid2FullInfoRDD.mapToPair((myTuple) -> {
			Long userid = myTuple._1;
			Row userInfoRow = myTuple._2._2;
			String sessionInfo = myTuple._2._1;
			String sessionid = StringUtils.getFieldFromConcatString(sessionInfo, "\\|", Constant.FIELD_SESSION_ID);
			int age = userInfoRow.getInt(3);
			String professional = userInfoRow.getString(4);
			String city = userInfoRow.getString(5);
			String sex = userInfoRow.getString(6);

			String fullAggrInfo = sessionInfo + "|" + Constant.FIELD_AGE + "=" + age + "|" + Constant.FIELD_PROFESSIONAL
					+ "=" + professional + "|" + Constant.FIELD_CITY + "=" + city + "|" + Constant.FIELD_SEX + "="
					+ sex;

			return new Tuple2<String, String>(sessionid, fullAggrInfo);
		});

		return null;

	}

}
