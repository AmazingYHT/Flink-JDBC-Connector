package org.sink.enums;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 *@ClassName SinkSource
 *@Description TODO
 *@Author yonght
 *@Date 2023/9/5 17:53
 **/
public enum SinkSourceEnums {
    MYSQL("mysql", "com.mysql.jdbc.Driver"),
    CLICKHOUSE("clickhouse", "ru.yandex.clickhouse.ClickHouseDriver"),
    ;

    private String type;
    private String driver;

    SinkSourceEnums(String type, String driver) {
        this.type = type;
        this.driver = driver;
    }

    public String getType() {
        return type;
    }

    public String getDriver() {
        return driver;
    }

    private static final Map<String, SinkSourceEnums> map = new HashMap<>();

    static {
        for (SinkSourceEnums s : values()) {
            map.put(s.getType(), s);
        }
    }

    public static Map<String, SinkSourceEnums> getMap() {
        Map<String, SinkSourceEnums> retMap = new HashMap<>();
        retMap.putAll(map);
        return retMap;
    }

    public static String getDriverByType(String type) {
        return Optional.ofNullable(map.get(type)).map(SinkSourceEnums::getDriver).orElse("");
    }

}
