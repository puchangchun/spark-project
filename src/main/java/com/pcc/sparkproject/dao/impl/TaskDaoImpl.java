package com.pcc.sparkproject.dao.impl;

import java.sql.ResultSet;
import java.sql.SQLException;

import com.pcc.sparkproject.dao.ITaskDao;
import com.pcc.sparkproject.domian.Task;
import com.pcc.sparkproject.jdbc.JDBCHelper;

public class TaskDaoImpl implements ITaskDao{

	/**
	 * 根据之间查询task
	 */
	@Override
	public Task findById(long task_id) {
		Task task = new Task();
		String sql = "select * from task where task_id = ?";
		JDBCHelper.getInstance().executeQurey(sql,(ResultSet rs)->{
			try {
				if (rs.next()) {
					task.setTaskid(rs.getLong(1));
					task.setTaskName(rs.getString(2));
					task.setCreateTime(rs.getString(3));
					task.setStartTime(rs.getString(4));
					task.setFinishTime(rs.getString(5));
					task.setTaskType(rs.getString(6));
					task.setTaskStatus(rs.getString(7));
					task.setTaskParam(rs.getString(8));
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}, task_id);	
		return task;
	}

}
