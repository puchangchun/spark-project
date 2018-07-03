package com.pcc.sparkproject.spark;

import com.alibaba.fastjson.JSONObject;
import com.pcc.sparkproject.conf.ConfigrationManager;
import com.pcc.sparkproject.constant.Constants;
import com.pcc.sparkproject.dao.ISessionAggrStatDAO;
import com.pcc.sparkproject.dao.ITaskDao;
import com.pcc.sparkproject.dao.impl.DaoFactory;
import com.pcc.sparkproject.domian.SessionAggrStat;
import com.pcc.sparkproject.domian.Task;
import com.pcc.sparkproject.test.MockData;
import com.pcc.sparkproject.util.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Date;

/**
 * 用户访问session分析的spark任务
 * 1、时间范围：起始日期~结束日期
 * 2、性别：男或女
 * 3、年龄范围
 * 4、职业：多选
 * 5、城市：多选
 * 6、搜索词：多个搜索词，只要某个session中的任何一个action搜索过指定的关键词，那么session就符合条件
 * 7、点击品类：多个品类，只要某个session中的任何一个action点击过某个品类，那么session就符合条件
 *
 * @author 99653
 */
public class UserVisitSessionAnalyzeSpark {

    public static void main(String[] args) {
        args = new String[]{"1"};

        // 得到spark上下文
        SparkConf conf = new SparkConf().setAppName(Constants.SPARK_APP_NAME_SESSION).setMaster("local");
        SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext());
        // 数据层的组件
        ITaskDao taskDao = DaoFactory.getTaskDao();
        // 是否模拟数据,模拟数据中的日期为当天的，需要需改mysql task表的paramJS中的日期范围包含当天才能获取
        mock(sc, sparkSession);
        // 根据传递的参数拿到taskid
        Long taskid = ParamUtils.getTaskIdFromArgs(args);
        // 取数据到DAO
        Task task = taskDao.findById(taskid);
        // 得到人物参数的JSON对象
        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());
        // 根据时间参数拿到用户行为表的RDD
        JavaRDD<Row> actionRDD = getActionRDDByDateRange(sparkSession, taskParam);

        /**
         * 下面要将用户行为数据和用户信息数据的两表进行聚合 两表通过user_id关联 最后的结果是：session_id ---- 多种行为 + 用户信息
         * 一个session又多种行为，所以先要将一个session做聚合，把多种行为聚合在一起 再将session对应的用户信息关联上
         * 聚合的时候同时计算访问时长和步长
         */


        /**
         * 信息聚合
         * 聚合成user-id --> 用户session合并信息 + 访问时间和步长的的计算
         */
        JavaPairRDD<String, String> sessionid2ActionInfoRDD = aggregateBySession(sparkSession, actionRDD);

        //自定义累加器
        SessionAggrStatAccumulator sessionAggrStatAccumulator = new SessionAggrStatAccumulator();

        //用自定义累加器
        sc.sc().register(sessionAggrStatAccumulator, "mySessionAcc");

        /*
        得到过滤后的RDD
        因为过滤会遍历整个RDD
        所以可以同时进行不同session的统计
        尽量在一个算子中实现多种功能
         */
        JavaPairRDD<String, String> filterSessionRDD = filterSession(sessionid2ActionInfoRDD, taskParam, sessionAggrStatAccumulator);

/*        for (Tuple2<String, String> tuple2 : filterSessionRDD.take(50)) {
            System.out.println(tuple2._1 + ":" + tuple2._2);
        }*/

        System.out.println(filterSessionRDD.collect().size()+"------filter size-----");
        /**
         * 要ActionRDD之后才会执行acc累加器
         */
        System.out.println(sessionAggrStatAccumulator.value());

        /**
         * 定义写入mySql--session-aggr-stat表中方法
         * 该表是对过滤后session的统计
         */
        writeSessionAggrTable(sessionAggrStatAccumulator.value(),taskid);

        // 关闭spark上下文
        sc.close();

    }

    /**
     * 把统计后的结果写入数据库
     *
     * @param value 累加器获取的统计值
     */
    private static void writeSessionAggrTable(String value,Long taskid) {
        double session_count = Double.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.SESSION_COUNT));

        long visit_length_1s_3s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_1s_3s));
        long visit_length_4s_6s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_4s_6s));
        long visit_length_7s_9s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_7s_9s));
        long visit_length_10s_30s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_10s_30s));
        long visit_length_30s_60s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_30s_60s));
        long visit_length_1m_3m = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_1m_3m));
        long visit_length_3m_10m = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_3m_10m));
        long visit_length_10m_30m = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_10m_30m));
        long visit_length_30m = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_30m));

        long step_length_1_3 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_1_3));
        long step_length_4_6 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_4_6));
        long step_length_7_9 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_7_9));
        long step_length_10_30 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_10_30));
        long step_length_30_60 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_30_60));
        long step_length_60 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_60));

        // 计算各个访问时长和访问步长的范围
        double visit_length_1s_3s_ratio = NumberUtils.formatDouble(
                visit_length_1s_3s / session_count, 4);
        double visit_length_4s_6s_ratio = NumberUtils.formatDouble(
                visit_length_4s_6s / session_count, 4);
        double visit_length_7s_9s_ratio = NumberUtils.formatDouble(
                visit_length_7s_9s / session_count, 4);
        double visit_length_10s_30s_ratio = NumberUtils.formatDouble(
                visit_length_10s_30s / session_count, 4);
        double visit_length_30s_60s_ratio = NumberUtils.formatDouble(
                visit_length_30s_60s / session_count, 4);
        double visit_length_1m_3m_ratio = NumberUtils.formatDouble(
                visit_length_1m_3m / session_count, 4);
        double visit_length_3m_10m_ratio = NumberUtils.formatDouble(
                visit_length_3m_10m / session_count, 4);

        double testdouble  =visit_length_10m_30m /session_count;
        double visit_length_10m_30m_ratio = NumberUtils.formatDouble(
                visit_length_10m_30m / session_count, 4);
        double visit_length_30m_ratio = NumberUtils.formatDouble(
                visit_length_30m / session_count, 4);

        double step_length_1_3_ratio = NumberUtils.formatDouble(
                step_length_1_3 / session_count, 4);
        double step_length_4_6_ratio = NumberUtils.formatDouble(
                step_length_4_6 / session_count, 4);
        double step_length_7_9_ratio = NumberUtils.formatDouble(
                step_length_7_9 / session_count, 4);
        double step_length_10_30_ratio = NumberUtils.formatDouble(
                step_length_10_30 / session_count, 4);
        double step_length_30_60_ratio = NumberUtils.formatDouble(
                step_length_30_60 / session_count, 4);
        double step_length_60_ratio = NumberUtils.formatDouble(
                step_length_60 / session_count, 4);

        // 将统计结果封装为Domain对象
        SessionAggrStat sessionAggrStat = new SessionAggrStat();
        sessionAggrStat.setTaskid(taskid);
        sessionAggrStat.setSession_count(Math.round(session_count));
        sessionAggrStat.setVisit_length_1s_3s_ratio(visit_length_1s_3s_ratio);
        sessionAggrStat.setVisit_length_4s_6s_ratio(visit_length_4s_6s_ratio);
        sessionAggrStat.setVisit_length_7s_9s_ratio(visit_length_7s_9s_ratio);
        sessionAggrStat.setVisit_length_10s_30s_ratio(visit_length_10s_30s_ratio);
        sessionAggrStat.setVisit_length_30s_60s_ratio(visit_length_30s_60s_ratio);
        sessionAggrStat.setVisit_length_1m_3m_ratio(visit_length_1m_3m_ratio);
        sessionAggrStat.setVisit_length_3m_10m_ratio(visit_length_3m_10m_ratio);
        sessionAggrStat.setVisit_length_10m_30m_ratio(visit_length_10m_30m_ratio);
        sessionAggrStat.setVisit_length_30m_ratio(visit_length_30m_ratio);
        sessionAggrStat.setStep_length_1_3_ratio(step_length_1_3_ratio);
        sessionAggrStat.setStep_length_4_6_ratio(step_length_4_6_ratio);
        sessionAggrStat.setStep_length_7_9_ratio(step_length_7_9_ratio);
        sessionAggrStat.setStep_length_10_30_ratio(step_length_10_30_ratio);
        sessionAggrStat.setStep_length_30_60_ratio(step_length_30_60_ratio);
        sessionAggrStat.setStep_length_60_ratio(step_length_60_ratio);

        DaoFactory.getSessionAggrStatDAO().insert(sessionAggrStat);
    }

    /**
     * 根据taskParam条件来过滤聚合后的sessionRDD
     * 并且统计访问时长与步长
     *
     * @param sessionid2ActionInfoRDD
     * @param taskParam
     * @param sessionAggrStatAccumulator
     * @return
     */
    private static JavaPairRDD<String, String>
    filterSession(JavaPairRDD<String, String> sessionid2ActionInfoRDD,
                  JSONObject taskParam, SessionAggrStatAccumulator sessionAggrStatAccumulator) {

        //取出给定参数
        String startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE);
        String endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE);
        String professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS);
        String cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES);
        String sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX);
        String keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS);
        String categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS);

        //进行拼接，要判断是否为null
        String parameter = (startAge != null ? Constants.PARAM_START_AGE + "=" + startAge + "|" : "")
                + (endAge != null ? Constants.PARAM_END_AGE + "=" + endAge + "|" : "")
                + (professionals != null ? Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" : "")
                + (cities != null ? Constants.PARAM_CITIES + "=" + cities + "|" : "")
                + (sex != null ? Constants.PARAM_SEX + "=" + sex + "|" : "")
                + (keywords != null ? Constants.PARAM_KEYWORDS + "=" + keywords + "|" : "")
                + (categoryIds != null ? Constants.PARAM_CATEGORY_IDS + "=" + categoryIds : "");

        //可能会有| 需要去掉| 来调用辅助类
        if (parameter.endsWith("|")) {
            parameter = parameter.substring(0, parameter.length() - 1);
        }

        final String _parameter = parameter;

        JavaPairRDD<String, String> filterSessionInfoRDD = sessionid2ActionInfoRDD.filter(new Function<Tuple2<String, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, String> myTuple2) throws Exception {
                //取出session信息
                String aggrInfo = myTuple2._2;

                //如果不在这个范围between返回false,这时候这就返回false过滤掉这个aggrSession

                // 按照年龄范围进行过滤（startAge、endAge）
                if (!ValidUtils.between(aggrInfo, Constants.FIELD_AGE, _parameter, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) {
                    return false;
                }
                //按照职业来过滤
                if (!ValidUtils.in(aggrInfo, Constants.FIELD_PROFESSIONAL, _parameter, Constants.PARAM_PROFESSIONALS)) {
                    return false;
                }
                //按照城市来过滤
                if (!ValidUtils.in(aggrInfo, Constants.FIELD_CITY, _parameter, Constants.PARAM_CITIES)) {
                    return false;
                }
                //按照性别过滤
                if (!ValidUtils.equal(aggrInfo, Constants.FIELD_SEX, _parameter, Constants.PARAM_SEX)) {
                    return false;
                }
                //按照搜索词过滤
                if (!ValidUtils.in(aggrInfo, Constants.FIELD_SEARCH_KEYWORDS, _parameter, Constants.PARAM_KEYWORDS)) {
                    return false;
                }
                //按照点击品类过滤
                if (!ValidUtils.in(aggrInfo, Constants.FIELD_CLICK_CATEGORY_IDS, _parameter, Constants.PARAM_CATEGORY_IDS)) {
                    return false;
                }


                // 主要走到这一步，那么就是需要计数的session
                sessionAggrStatAccumulator.add(Constants.SESSION_COUNT);

                // 计算出session的访问时长和访问步长的范围，并进行相应的累加
                String visitLengthS = StringUtils.getFieldFromConcatString(
                        aggrInfo, "\\|", Constants.FIELD_VISIT_LENGTH);
                String stepLengthS = StringUtils.getFieldFromConcatString(
                        aggrInfo, "\\|", Constants.FIELD_STEP_LENGTH);
                Long visitLength = null;
                Long stepLength = null;
                if (StringUtils.isNotEmpty(visitLengthS)) {
                    visitLength = Long.valueOf(visitLengthS);
                }
                if (StringUtils.isNotEmpty(stepLengthS)) {
                    stepLength = Long.valueOf(stepLengthS);
                }
                if (visitLength != null) {
                    calculateVisitLength(visitLength);
                }
                if (stepLength != null) {
                    calculateStepLength(stepLength);
                }

                return true;
            }

            /**
             * 计算访问时长范围
             * @param visitLength
             */
            private void calculateVisitLength(long visitLength) {
                if (visitLength >= 1 && visitLength <= 3) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1s_3s);
                } else if (visitLength >= 4 && visitLength <= 6) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_4s_6s);
                } else if (visitLength >= 7 && visitLength <= 9) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_7s_9s);
                } else if (visitLength >= 10 && visitLength <= 30) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10s_30s);
                } else if (visitLength > 30 && visitLength <= 60) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30s_60s);
                } else if (visitLength > 60 && visitLength <= 180) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1m_3m);
                } else if (visitLength > 180 && visitLength <= 600) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_3m_10m);
                } else if (visitLength > 600 && visitLength <= 1800) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10m_30m);
                } else if (visitLength > 1800) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30m);
                }
            }

            /**
             * 计算访问步长范围
             * @param stepLength
             */
            private void calculateStepLength(long stepLength) {
                if (stepLength >= 1 && stepLength <= 3) {
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_1_3);
                } else if (stepLength >= 4 && stepLength <= 6) {
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_4_6);
                } else if (stepLength >= 7 && stepLength <= 9) {
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_7_9);
                } else if (stepLength >= 10 && stepLength <= 30) {
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_10_30);
                } else if (stepLength > 30 && stepLength <= 60) {
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_30_60);
                } else if (stepLength > 60) {
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_60);
                }
            }
        });


        return filterSessionInfoRDD;
    }

    /**
     * 是否需要生成本地数据进行调试
     *
     * @param sc
     * @param sparkSession
     */
    private static void mock(JavaSparkContext sc, SparkSession sparkSession) {
        if (ConfigrationManager.getBoolean(Constants.SPARK_LOCAL)) {
            // 本地
            MockData.mock(sc, sparkSession);
        }
    }

    /**
     * 2.0以后sparkSession HIVECONTEXT SPARKCONTEXT合并在SPARKSESION 分本地和生产环境 获取sparkSession
     *
     * @return
     */
/*    public static sparkSession getsparkSession(JavaSparkContext sc) {
        if (ConfigrationManager.getBoolean(Constants.SPARK_LOCAL)) {
            return new sparkSession(sc);
        } else {
            return new HiveContext(sc);
        }
    }*/

    /**
     * 通过数据库查询出来的params，来获取时间参数 并且得到指定时间范围用户访问行为数据
     *
     * @param sparkSession
     * @param paramJS
     * @return
     */
    public static JavaRDD<Row> getActionRDDByDateRange(SparkSession sparkSession, JSONObject paramJS) {

        // 拿到时间范围
        String startDate = ParamUtils.getParam(paramJS, Constants.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(paramJS, Constants.PARAM_END_DATE);
        // 建立SQL语句
        String sql =
                "select * "
                        + "from user_visit_action "
                        + "where date>='" + startDate + "' "
                        + "and date<='" + endDate + "'";

        // 执行
        Dataset<Row> datasets = sparkSession.sql(sql);
        return datasets.toJavaRDD();
    }

    /**
     * 获取所有用户信息的RDD
     *
     * @param sparkSession
     * @return
     */
    public static JavaRDD<Row> getUserInfoRDD(SparkSession sparkSession) {
        String sql = "select * from user_info";
        // 执行
        Dataset<Row> datasets = sparkSession.sql(sql);
        return datasets.toJavaRDD();
    }

    /**
     * 按照session粒度进行聚合
     *
     * @param actionRDD 用户行为的元素数据RDD
     * @return
     */
    public static JavaPairRDD<String, String> aggregateBySession(SparkSession sparkSession, JavaRDD<Row> actionRDD) {
        // Row相当于数据库表中的一行数据
        // 转换成PairRDD key - value
        // mapToPair对每个Row通过函数转换成Tuple<KEY,VALUE>
        JavaPairRDD<String, Row> sessionidPairRDD = actionRDD.mapToPair((row) -> {
            return new Tuple2<>(row.getString(2), row);
        });
        System.out.println(sessionidPairRDD.count() + " sessionid + string");


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

            // session的起始和结束时间
            Date startTime = null;
            Date endTime = null;
            // session的访问步长
            int stepLength = 0;

            // 遍历每一种行为，一种行为对应一个Row
            for (Row row : tuple._2) {
                // 获取该session的userid
                if (userid == null) {
                    userid = row.getLong(1);
                }
                String searchKeyWord = row.getString(5);
                Long clickCategoryId = row.isNullAt(6) ? null : row.getLong(6);
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

                // 计算session开始和结束时间
                Date actionTime = DateUtils.parseTime(row.getString(4));

                if (startTime == null) {
                    startTime = actionTime;
                }
                if (endTime == null) {
                    endTime = actionTime;
                }

                if (actionTime.before(startTime)) {
                    startTime = actionTime;
                }
                if (actionTime.after(endTime)) {
                    endTime = actionTime;
                }

                // 计算session访问步长
                stepLength++;
            }

            String searchKeyWords = StringUtils.trimComma(sSearchWordBuffer.toString());
            String clickCategoryIds = StringUtils.trimComma(sClickCategoryIdsBuffer.toString());

            // 计算session访问时长（秒）
            long visitLength = (endTime.getTime() - startTime.getTime()) / 1000;

            // 这里完成后下一步需要和userInfo链接，为了方便这里可以把key改成userid
            // <userid,row>-+-+<userid,string> --> <sessionid,string>
            // string的数据格式为 key=value|key=value
            String partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionid + "|"
                    + Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeyWords + "|"
                    + Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds + "|"
                    + Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|"
                    + Constants.FIELD_STEP_LENGTH + "=" + stepLength;

            // 返回<userid,string (sessionid=?|searchkeywords=?|..)>
            return new Tuple2<>(userid, partAggrInfo);
        });


        // 得到userInfoRDD
        JavaRDD<Row> userInfoRDD = getUserInfoRDD(sparkSession);
        // 转化为key-value
        JavaPairRDD<Long, Row> userid2InfoRDD = userInfoRDD.mapToPair((row) -> {
            return new Tuple2<>(row.getLong(0), row);
        });

        // 通过key进行拼接
        JavaPairRDD<Long, Tuple2<String, Row>> userid2FullInfoRDD = userid2partActionRDD.join(userid2InfoRDD);

        System.out.println(userid2partActionRDD.count() + "userid + aggrInfo ");
        // 处理拼接后的数据
        JavaPairRDD<String, String> sessionid2FullAggrInfoRDD = userid2FullInfoRDD.mapToPair((myTuple) -> {
            Long userid = myTuple._1;
            Row userInfoRow = myTuple._2._2;
            String sessionInfo = myTuple._2._1;
            String sessionid = StringUtils.getFieldFromConcatString(sessionInfo, "\\|", Constants.FIELD_SESSION_ID);
            int age = userInfoRow.getInt(3);
            String professional = userInfoRow.getString(4);
            String city = userInfoRow.getString(5);
            String sex = userInfoRow.getString(6);

            String fullAggrInfo = sessionInfo + "|" + Constants.FIELD_AGE + "=" + age + "|" + Constants.FIELD_PROFESSIONAL
                    + "=" + professional + "|" + Constants.FIELD_CITY + "=" + city + "|" + Constants.FIELD_SEX + "="
                    + sex;

            return new Tuple2<String, String>(sessionid, fullAggrInfo);
        });

        return sessionid2FullAggrInfoRDD;

    }

}
