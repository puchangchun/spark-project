package com.pcc.sparkproject.test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import com.carrotsearch.hppc.cursors.ObjectCharCursor;
import com.pcc.sparkproject.jdbc.JDBCHelper;

public class JDBCTest {

	public static void main(String[] args) {
		select();
	}
	
	/**
	 * 测试查询数据
	 */
	private static void select() {
		JDBCHelper.getInstance().executeQurey("select * from test where id = ?",(ResultSet rs) -> {
			// 获取到ResultSet以后，就需要对其进行遍历，然后获取查询出来的每一条数据
			try {
				while(rs.next()) {
					String name = rs.getString(1);
					System.out.println(", name=" + name);    
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		},1);
		
	}
	private static void update() {
		List<Object[]> list = new ArrayList<Object[]>();

		JDBCHelper.getInstance().executeBatch("insert into test(id,name) values(?,?)",list)  ;
	}
}
