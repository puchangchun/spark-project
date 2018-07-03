package com.pcc.sparkproject.dao.impl;

import com.pcc.sparkproject.dao.ISessionAggrStatDAO;
import com.pcc.sparkproject.domian.SessionAggrStat;
import com.pcc.sparkproject.jdbc.JDBCHelper;

/**
 * Dao接口的实现类
 * 业务分离
 */
public class SessionAggrStatDAOImpl implements ISessionAggrStatDAO {
    @Override
    public void insert(SessionAggrStat sessionAggrStat) {
        String sql = "insert into session_aggr_stat values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        JDBCHelper.getInstance().executeUpdate(sql,sessionAggrStat.getTaskid(),
                sessionAggrStat.getSession_count(),
                sessionAggrStat.getVisit_length_1s_3s_ratio(),
                sessionAggrStat.getVisit_length_4s_6s_ratio(),
                sessionAggrStat.getVisit_length_7s_9s_ratio(),
                sessionAggrStat.getVisit_length_10s_30s_ratio(),
                sessionAggrStat.getVisit_length_30s_60s_ratio(),
                sessionAggrStat.getVisit_length_1m_3m_ratio(),
                sessionAggrStat.getVisit_length_3m_10m_ratio(),
                sessionAggrStat.getVisit_length_10m_30m_ratio(),
                sessionAggrStat.getVisit_length_30m_ratio(),
                sessionAggrStat.getStep_length_1_3_ratio(),
                sessionAggrStat.getStep_length_4_6_ratio(),
                sessionAggrStat.getStep_length_7_9_ratio(),
                sessionAggrStat.getStep_length_10_30_ratio(),
                sessionAggrStat.getStep_length_30_60_ratio(),
                sessionAggrStat.getStep_length_60_ratio());
    }
}
