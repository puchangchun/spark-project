package com.pcc.sparkproject.constant;

/**
 * 接口中的变量为 public static final 函数为 public abstract
 * 
 * @author 99653
 *
 */
public interface Constant {
	/**
	 * 项目相关的常量
	 */
	String JDBC_DRIVER = "jdbc.driver";
	String JDBC_DATASOURCE_SIZE = "jdbc.datasource.size";
	String JDBC_URL = "jdbc.url";
	String JDBC_USER = "jdbc.user";
	String JDBC_PASSWORD = "jdbc.password";
	String SPARK_LOCAL = "spark.local";

	/**
	 * spark作业的常量
	 * session的聚合里出现的字段
	 */
	String SPARK_APP_NAME_SESSION = "UserVisitSessionAnalyzeSpark";
	String FIELD_SESSION_ID = "sessionid";
	String FIELD_SEARCH_KEYWORDS = "searchKeywords";
	String FIELD_CLICK_CATEGORY_IDS = "clickCategoryIds";
	String FIELD_AGE = "age";
	String FIELD_PROFESSIONAL = "professional";
	String FIELD_CITY = "city";
	String FIELD_SEX = "sex";
	
	
	/**
	 * 任务相关的常量
	 * 出现在task中的param参数里出现的字段
	 */
	String PARAM_START_DATE = "startDate";
	String PARAM_END_DATE = "endDate";
	String PARAM_START_AGE= "startAge";
	String PARAM_END_AGE = "endAge";
	String PARAM_PROFESSIONALS = "professionals";
	String PARAM_CITIES = "cites";
	String PARAM_SEX = "sex";
	String PARAM_KEYWORDS = "keywords";
	String PARAM_CATEGORY_IDS = "categoryIds";

}
