package org.sink;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.sink.common.Constants;
import org.sink.enums.SinkSourceEnums;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @Author yonght
 * @Description
 * @Date 2023/9/5 16:37 
 * @return
 **/
public class SinkSingleJdbc<T> {

    private final static String NA = "null";
    private final SinkFunction<T> sink;

    /**提交重试次数**/
    public static final String DEFAULT_MAX_RETRY_TIMES = "3";
    /**提交条件之：间隔**/
    private static final String DEFAULT_INTERVAL_MILLIS = "0";
    /**提交条件之：数据量**/
    public static final String DEFAULT_SIZE = "5000";

    /**
     * 获取clickhouse sinkFunction
     *
     * @param sql                  插入语句，格式必须为  inert into table  a,b values (?,?)
     * @param jdbcStatementBuilder 如何用单条信息填充sql
     */
    public SinkSingleJdbc(String sql, JdbcStatementBuilder<T> jdbcStatementBuilder,
                          Properties props) {
        sink = JdbcSink.sink(
                sql,
                jdbcStatementBuilder,
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:" + props.getProperty(Constants.DRIVE) + "://" + props.getProperty(Constants.HOST) + "/" + props.getProperty(Constants.DATABASE))
                        .withDriverName(SinkSourceEnums.getDriverByType(props.getProperty(Constants.DRIVE)))
                        .withUsername(props.getProperty(Constants.USERNAME))
                        .withPassword(props.getProperty(Constants.PASSWORD))
                        .build()
        );
    }

    /**
     * 获取clickhouse sinkFunction
     *
     * @param sql                  插入语句，格式必须为  inert into table  a,b values (?,?)
     * @param jdbcStatementBuilder 如何用单条信息填充sql
     * @param props
     */
    public SinkSingleJdbc(String sql, JdbcStatementBuilder<T> jdbcStatementBuilder,
                          Properties props, Boolean options) {
        sink = JdbcSink.sink(
                sql,
                jdbcStatementBuilder,
                JdbcExecutionOptions.builder()
                        .withBatchIntervalMs(Long.valueOf(props.getProperty(Constants.BATCH_INTERVAL_MS, DEFAULT_INTERVAL_MILLIS)))
                        .withBatchSize(Integer.valueOf(props.getProperty(Constants.BATCH_SIZE, DEFAULT_SIZE)))
                        .withMaxRetries(Integer.valueOf(props.getProperty(Constants.MAX_RETRIES, DEFAULT_MAX_RETRY_TIMES)))
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:" + props.getProperty(Constants.DRIVE) + "://" + props.getProperty(Constants.HOST) + "/" + props.getProperty(Constants.DATABASE))
                        .withDriverName(SinkSourceEnums.getDriverByType(props.getProperty(Constants.DRIVE)))
                        .withUsername(props.getProperty(Constants.USERNAME))
                        .withPassword(props.getProperty(Constants.PASSWORD))
                        .build()
        );
    }

    public SinkFunction<T> getSink() {
        return sink;
    }


    public static void setPs(PreparedStatement ps, Field[] fields, Object bean) throws IllegalAccessException, SQLException {
        for (int i = 1; i <= fields.length; i++) {
            Field field = fields[i - 1];
            field.setAccessible(true);
            Object o = field.get(bean);
            if (o == null) {
                ps.setNull(i, 0);
                continue;
            }
            format(ps, i, o);
        }
    }


    public static void setPs(PreparedStatement ps, LinkedHashMap<String, Object> fields) throws IllegalAccessException, SQLException {
        int i = 1;
        for (Map.Entry<String, Object> entry : fields.entrySet()) {
            Object o = entry.getValue();
            if (o == null) {
                ps.setNull(i, 0);
                continue;
            }
            format(ps, i, o);
            i++;
        }
    }


    private static void format(PreparedStatement ps, int i, Object obj) throws SQLException {
        String fieldValue = obj.toString();
        if (!NA.equals(fieldValue) && !"".equals(fieldValue)) {
            ps.setObject(i, fieldValue);
        } else {
            ps.setNull(i, 0);
        }
    }
}
