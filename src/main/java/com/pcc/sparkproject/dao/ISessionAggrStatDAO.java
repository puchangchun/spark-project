package com.pcc.sparkproject.dao;

import com.pcc.sparkproject.domian.SessionAggrStat;

/**
 * session_aggr_stat 表的DAO接口
 * 业务层面向接口编程
 */
public interface ISessionAggrStatDAO {
    public void insert(SessionAggrStat sessionAggrStat);
}
