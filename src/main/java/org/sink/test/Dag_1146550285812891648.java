package org.sink.test;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.sink.SinkSingleJdbc;
import org.sink.common.Constants;
import org.sink.enums.SinkSourceEnums;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @Author yonght
 * @Description
 * @Date 2023/9/6 9:35
 * @Param  * @param null
 * @return
 **/
public class Dag_1146550285812891648 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //开启checkpoint自我恢复
        CheckpointConfig config = env.getCheckpointConfig();
        config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);

        //自我失败恢复设定
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                // 每个时间间隔的最大故障次数
                300,
                // 测量故障率的时间间隔
                org.apache.flink.api.common.time.Time.of(5, TimeUnit.MINUTES),
                // 延时
                org.apache.flink.api.common.time.Time.of(10, TimeUnit.SECONDS)
        ));

        //OutPutBean newOutPutBean = new OutPutBean();
        //Map<String, Object> props = new HashMap();
        //props.put("code", "111141435612272787456");
        //props.put("name", "D2032");
        //props.put("status", 1);
        //newOutPutBean.setProps(props);

        //DataStream<OutPutBean> midPlatformStream1146550285854834688 = env.fromElements(newOutPutBean);

        DataStream<OutPutBean> midPlatformStream1146550285854834688 = env.addSource(new SourceFunction<OutPutBean>() {
            private volatile boolean isRunning = true;

            @Override
            public void run(SourceContext<OutPutBean> ctx) throws Exception {
                int i = 1;
                while (isRunning) {
                    OutPutBean newOutPutBean = new OutPutBean();
                    Map<String, Object> props = new HashMap<>();
                    props.put("code", "111141435612272787456" + i);
                    props.put("name", "D2032");
                    props.put("status", i);
                    newOutPutBean.setProps(props);
                    ctx.collect(newOutPutBean);
                    Thread.sleep(10); // 每秒发送一次数据
                    i++;
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        });


        Properties sinkerProps = new Properties();
        sinkerProps.put(Constants.HOST, "127.0.0.1:3306");
        sinkerProps.put(Constants.DATABASE, "test");
        sinkerProps.put(Constants.USERNAME, "root");
        sinkerProps.put(Constants.PASSWORD, "root");
        sinkerProps.put(Constants.DRIVE, SinkSourceEnums.MYSQL.getType());
        //批量提交
        sinkerProps.put(Constants.BATCH_INTERVAL_MS, "5000");
        sinkerProps.put(Constants.BATCH_SIZE, "1000");
        sinkerProps.put(Constants.MAX_RETRIES, "2");

        //sinkerProps.put(Constants.HOST, "127.0.0.1:8135");
        //sinkerProps.put(Constants.DATABASE, "test");
        //sinkerProps.put(Constants.USERNAME, "root");
        //sinkerProps.put(Constants.PASSWORD, "root");
        //sinkerProps.put(Constants.DRIVE, SinkSourceEnums.CLICKHOUSE.getType());

        SinkFunction<OutPutBean> sinkClickhouse = new SinkSingleJdbc<>("insert into test10 (code, name, status, data_time ) values (?,?,?,?)",
                new OutputMapFunction(),
                sinkerProps, true)
                .getSink();

        midPlatformStream1146550285854834688.addSink(sinkClickhouse);
        env.execute("stream_task_1146550285812891648");
    }
}