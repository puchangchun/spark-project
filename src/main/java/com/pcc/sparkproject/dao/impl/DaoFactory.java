package com.pcc.sparkproject.dao.impl;

import com.pcc.sparkproject.dao.ITaskDao;

/**
 * Dao接口的工厂，解耦 业务和代码逻辑
 * @author 99653
 *
 */
public class DaoFactory {

	public static ITaskDao getTaskDao() {
		
		return new TaskDaoImpl();
		
	}
}
