package com.pcc.sparkproject.spark;


import com.pcc.sparkproject.constant.Constants;
import com.pcc.sparkproject.util.StringUtils;
import org.apache.spark.util.AccumulatorV2;


/**
 * 自定义Accumulator来实现对session的统计
 * isZero: 当AccumulatorV2中存在类似数据不存在这种问题时，是否结束程序。
 * copy: 拷贝一个新的AccumulatorV2
 * reset: 重置AccumulatorV2中的数据
 * add: 操作数据累加方法实现
 * merge: 合并数据
 * value: AccumulatorV2对外访问的数据结果
 */
public class SessionAggrStatAccumulator extends AccumulatorV2<String, String> {

    public String sessionStatus = Constants.SESSION_COUNT + "=0|"
            + Constants.TIME_PERIOD_1s_3s + "=0|"
            + Constants.TIME_PERIOD_4s_6s + "=0|"
            + Constants.TIME_PERIOD_7s_9s + "=0|"
            + Constants.TIME_PERIOD_10s_30s + "=0|"
            + Constants.TIME_PERIOD_30s_60s + "=0|"
            + Constants.TIME_PERIOD_1m_3m + "=0|"
            + Constants.TIME_PERIOD_3m_10m + "=0|"
            + Constants.TIME_PERIOD_10m_30m + "=0|"
            + Constants.TIME_PERIOD_30m + "=0|"
            + Constants.STEP_PERIOD_1_3 + "=0|"
            + Constants.STEP_PERIOD_4_6 + "=0|"
            + Constants.STEP_PERIOD_7_9 + "=0|"
            + Constants.STEP_PERIOD_10_30 + "=0|"
            + Constants.STEP_PERIOD_30_60 + "=0|"
            + Constants.STEP_PERIOD_60 + "=0";

    public StringBuffer sessionStatusStringBuffer = new StringBuffer(sessionStatus);

    @Override
    public boolean isZero() {
        if (sessionStatusStringBuffer.toString().equals(sessionStatus)) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public AccumulatorV2<String, String> copy() {
        SessionAggrStatAccumulator newSessionAggrStatAccumulator = new SessionAggrStatAccumulator();
        newSessionAggrStatAccumulator.sessionStatusStringBuffer = this.sessionStatusStringBuffer;
        return newSessionAggrStatAccumulator;
    }

    @Override
    public void reset() {
        sessionStatus = Constants.SESSION_COUNT + "=0|"
                + Constants.TIME_PERIOD_1s_3s + "=0|"
                + Constants.TIME_PERIOD_4s_6s + "=0|"
                + Constants.TIME_PERIOD_7s_9s + "=0|"
                + Constants.TIME_PERIOD_10s_30s + "=0|"
                + Constants.TIME_PERIOD_30s_60s + "=0|"
                + Constants.TIME_PERIOD_1m_3m + "=0|"
                + Constants.TIME_PERIOD_3m_10m + "=0|"
                + Constants.TIME_PERIOD_10m_30m + "=0|"
                + Constants.TIME_PERIOD_30m + "=0|"
                + Constants.STEP_PERIOD_1_3 + "=0|"
                + Constants.STEP_PERIOD_4_6 + "=0|"
                + Constants.STEP_PERIOD_7_9 + "=0|"
                + Constants.STEP_PERIOD_10_30 + "=0|"
                + Constants.STEP_PERIOD_30_60 + "=0|"
                + Constants.STEP_PERIOD_60 + "=0";
    }

    @Override
    public void add(String s) {

        // 使用StringUtils工具类，从v1中，提取v2对应的值，并累加1
        String oldValue = StringUtils.getFieldFromConcatString(sessionStatusStringBuffer.toString(), "\\|", s);
        if (StringUtils.isNotEmpty(oldValue)) {
            // 将范围区间原有的值，累加1
            int newValue = Integer.valueOf(oldValue) + 1;

            // 使用StringUtils工具类，将v1中，v2对应的值，设置成新的累加后的值
            StringUtils.setFieldInConcatString(sessionStatusStringBuffer, "\\|", s, String.valueOf(newValue));
        }
    }

    @Override
    public void merge(AccumulatorV2<String, String> accumulatorV2) {
        if (accumulatorV2 instanceof SessionAggrStatAccumulator){
            SessionAggrStatAccumulator sessionAggrStatAccumulator2 = (SessionAggrStatAccumulator) accumulatorV2;
            String sessionCount2 = sessionAggrStatAccumulator2.sessionStatusStringBuffer.toString();
            /**
             * 遍历每一个key-value
             */
            for (String strKeyValue : sessionCount2.split("\\|")){
                /**
                 * 执行加操作
                 */
                String[] strAry = strKeyValue.split("=");

                if (StringUtils.isNotEmpty(strAry[1])){
                    String value1 = StringUtils
                        .getFieldFromConcatString(sessionStatusStringBuffer.toString(), "\\|", strAry[0]);
                    int newValue = Integer.valueOf(strAry[1]) + Integer.valueOf(value1);
                    StringUtils.setFieldInConcatString(sessionStatusStringBuffer, "\\|",strAry[0],String.valueOf(newValue));
                }
            }
        }
    }

    @Override
    public String value() {
        return sessionStatusStringBuffer.toString();
    }
}

