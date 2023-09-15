### flink 配置多种数据源目前添加ck和mysql其余的可以自己添加自定义的也可以直接使用官方的
 当前工程可以直接打成jar丢到flink的依赖包中，然后直接当api调用就可以了，少了不少代码，insert语句
 需要自己转换成对应的语法


 CREATE SCHEMA IF NOT EXISTS test;
 USE test;
 CREATE TABLE IF NOT EXISTS test10 (
    id INT AUTO_INCREMENT PRIMARY KEY,
    code VARCHAR(255) NOT NULL,
    name VARCHAR(255) NOT NULL,
    status VARCHAR(255) NOT NULL,
    data_time DATETIME NOT NULL
);


CREATE SCHEMA IF NOT EXISTS test;
USE test;
CREATE TABLE IF NOT EXISTS test10 (
    id Int32 AUTO_INCREMENT PRIMARY KEY,
    code String,
    name String,
    status String,
    data_time DateTime
) ENGINE = MergeTree()
ORDER BY id;
