package com.pcc.sparkproject.dao;

import com.pcc.sparkproject.domian.Task;

/**
 * DAO接口,业务层面向接口编程，
 * @author 99653
 *
 */

public interface ITaskDao {
	Task findById(long taskid);
}
