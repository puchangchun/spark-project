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
	public Task findById(Long task_id) {
		final Task _task = new Task();
		String sql = "select * from task where task_id = ?";
		JDBCHelper.getInstance().executeQurey(sql,(ResultSet rs)->{
			try {
				if (rs.next()) {
					_task.setTaskid(rs.getLong(1));
					_task.setTaskName(rs.getString(2));
					_task.setCreateTime(rs.getString(3));
					_task.setStartTime(rs.getString(4));
					_task.setFinishTime(rs.getString(5));
					_task.setTaskType(rs.getString(6));
					_task.setTaskStatus(rs.getString(7));
					_task.setTaskParam(rs.getString(8));
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}, task_id);	
		return _task;
	}

}
