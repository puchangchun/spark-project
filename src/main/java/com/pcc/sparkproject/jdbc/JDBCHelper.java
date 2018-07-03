package com.pcc.sparkproject.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

import com.pcc.sparkproject.conf.ConfigrationManager;
import com.pcc.sparkproject.constant.Constants;

/**
 * 数据库连接池对象
 * 管理数据库的链接
 * 优化数据库的增删改查接口
 */
public class JDBCHelper {
	// 单列对象
	private static JDBCHelper jdbcHelper = null;

	// 数据库链接 ---LinkedList ---双向链表--- 顺序的插入和删除 较 ArrayList快
	private LinkedList<Connection> datasource = new LinkedList<>();

	private JDBCHelper() {
		// 获取配置文件中数据库连接池的大小
		int datasourceSize = ConfigrationManager.getInteger(Constants.JDBC_DATASOURCE_SIZE);

		// 初始化数据库连接池
		for (int i = 0; i < datasourceSize; i++) {
			String url = ConfigrationManager.getProperty(Constants.JDBC_URL);
			String name = ConfigrationManager.getProperty(Constants.JDBC_USER);
			String pwd = ConfigrationManager.getProperty(Constants.JDBC_PASSWORD);
			try {
				Connection connection = DriverManager.getConnection(url, name, pwd);
				datasource.push(connection);
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	// 项目，要尽量做成可配置的
	// 就是说，我们的这个数据库驱动，更进一步，也不只是放在常量接口中就可以了
	// 最好的方式，是放在外部的配置文件中，跟代码彻底分离
	// 常量接口中，只是包含了这个值对应的key的名字
	static {
		try {
			/**
			 * 通过配置文件，加载指定的数据库驱动
			 */
			Class.forName(ConfigrationManager.getProperty(Constants.JDBC_DRIVER));
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 获取单例对象的方法
	 * 
	 * @return jdbcHleper实力
	 */
	public static JDBCHelper getInstance() {

		if (jdbcHelper == null) {
			synchronized (JDBCHelper.class) {
				if (jdbcHelper == null) {
					jdbcHelper = new JDBCHelper();
				}
			}
		}
		return jdbcHelper;
	}
	
	/**
	 * 获取数据库链接，实现等待机制
	 * 如果线程都去这个方法拿数据,可能会返回null，所以需要做资源互斥
	 * @return
	 */
	public synchronized Connection getConnection() {
		while(datasource.size() == 0) {
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		//获取并且删除队列的头
		return datasource.poll();
	}
	
	/**
	 * 执行增删改sql
	 * @param sql
	 * @param params
	 * @return
	 */
	public int executeUpdate(String sql,Object... params) {
		int rtn = 0;
		Connection connection = null;
		PreparedStatement pstmt = null;
		try {
			connection = getConnection();
			pstmt = connection.prepareStatement(sql);
			for (int i = 0; i < params.length; i++) {	
				pstmt.setObject(i+1, params[i]);
			}
			rtn = pstmt.executeUpdate();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (connection!=null) {
				datasource.push(connection);
			}
		}
		return rtn;
	}
	
	/**
	 * 执行查询语句，并使用回调接口处理数据集
	 * @param sql
	 * @param queryCallback
	 * @param params
	 */
	public void executeQurey(String sql,QueryCallback queryCallback,Object... params) {
		Connection connection = null;
		PreparedStatement pStatement = null;
		ResultSet rSet = null;
		try {
			connection = getConnection();
			pStatement = connection.prepareStatement(sql);
			for (int i = 0; i < params.length; i++) {
				pStatement.setObject(i+1, params[i]);
			}
			rSet = pStatement.executeQuery();
			queryCallback.process(rSet);
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (connection != null) {
				
				datasource.push(connection);
			}
		}
	}
	
	/**
	 * 执行多条增删改语句
	 *	 批量执行SQL语句
	 * 
	 * 批量执行SQL语句，是JDBC中的一个高级功能
	 * 默认情况下，每次执行一条SQL语句，就会通过网络连接，向MySQL发送一次请求
	 * 
	 * 但是，如果在短时间内要执行多条结构完全一模一样的SQL，只是参数不同
	 * 虽然使用PreparedStatement这种方式，可以只编译一次SQL，提高性能，但是，还是对于每次SQL
	 * 都要向MySQL发送一次网络请求
	 * 
	 * 可以通过批量执行SQL语句的功能优化这个性能
	 * 一次性通过PreparedStatement发送多条SQL语句，比如100条、1000条，甚至上万条
	 * 执行的时候，也仅仅编译一次就可以
	 * 这种批量执行SQL语句的方式，可以大大提升性能
	 * @param sql
	 * @param paramsList
	 * @return
	 */
	public int[] executeBatch(String sql, List<Object[]> paramsList) {
		int[] rtn =null;
		Connection connection = null;
		PreparedStatement pStatement = null;
		
		try {
			connection = getConnection();
			
			//1:关闭connection对象的自动提交功能
			connection.setAutoCommit(false);
			
			pStatement = connection.prepareStatement(sql);
			
			//2:使用prepareStatement.addBatch加入批量的SQL参数
			for (Object[] params : paramsList) {
				for (int i = 0; i < params.length; i++) {
					pStatement.setObject(i+1, params[i]);
				}
				pStatement.addBatch();
				
			}
			
			//3:批量执行编译prepareStatement的语句,返回每条语句影响的行数
			rtn = pStatement.executeBatch();
			
			//4:提交sql语句到远程数据库执行
			connection.commit();
			
			//5:还原con
			connection.setAutoCommit(true);
		} catch (SQLException e) {
			e.printStackTrace();
		}finally {
			if (connection != null) {
				datasource.push(connection);
			}
		}
		
		return rtn;
	}
	
	/**
	 * 处理查询结果集的回调接口
	 * @author 99653
	 *
	 */
	public static interface QueryCallback{
		void process(ResultSet rSet);
	}
}






