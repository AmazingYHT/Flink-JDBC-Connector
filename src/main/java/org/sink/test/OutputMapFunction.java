package org.sink.test;

import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.sink.SinkSingleJdbc;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.LinkedHashMap;

/**
 * @Author yonght
 * @Description OutputMapFunction
 * @Date 2023/9/5 18:16 
 * @Param  * @param null
 * @return
 **/
public class OutputMapFunction implements JdbcStatementBuilder<OutPutBean> {

    private static final String YYYY_MM_DD_HH_MM_SS_SSS_PATTERN = "yyyy-MM-dd HH:mm:ss";

    private String convertDateByFormat(Long timeMillis, String inputTimePattern) {
        SimpleDateFormat df = new SimpleDateFormat(inputTimePattern);//定义格式，不显示毫秒
        Timestamp now = new Timestamp(timeMillis);//获取系统当前时间
        String str = df.format(now);

        return str;
    }

    @Override
    public void accept(PreparedStatement ps, OutPutBean outPutBean) throws SQLException {
        try {
            LinkedHashMap<String, Object> dataMap = new LinkedHashMap<>();
            dataMap.put("code", outPutBean.getProps().get("code"));
            dataMap.put("name", outPutBean.getProps().get("name"));
            dataMap.put("status", outPutBean.getProps().get("status"));
            Long requestTime = System.currentTimeMillis();
            String formattedRequestTime = convertDateByFormat(requestTime, YYYY_MM_DD_HH_MM_SS_SSS_PATTERN);
            dataMap.put("data_time", formattedRequestTime);
            SinkSingleJdbc.setPs(ps, dataMap);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
    }
}