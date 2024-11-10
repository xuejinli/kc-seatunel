# 常见问题解答

## 使用 SeaTunnel 需要安装 Spark 或者 Flink 这样的引擎么？
不需要，SeaTunnel 支持 Zeta、Spark 和 Flink 作为同步引擎的选择，您可以选择之一就行，社区尤其推荐使用 Zeta 这种专为同步场景打造的新一代超高性能同步引擎。Zeta 被社区用户亲切的称为 “泽塔奥特曼”! 
社区对 Zeta 的支持力度是最大的，功能也更丰富。

## SeaTunnel 支持哪些数据来源和数据目的地？
SeaTunnel 支持多种数据源来源和数据目的地，您可以在官网找到详细的列表：
SeaTunnel 支持的数据来源(Source)列表：https://seatunnel.apache.org/docs/connector-v2/source
SeaTunnel 支持的数据目的地(Sink)列表：https://seatunnel.apache.org/docs/connector-v2/sink

## 目前支持哪些数据源的 CDC ？
目前支持 MongoDB CDC、MySQL CDC、Opengauss CDC、Oracle CDC、PostgreSQL CDC、Sql Server CDC、TiDB CDC等，更多请查阅[Source](https://seatunnel.apache.org/docs/connector-v2/source)。

## 支持从 MySQL 备库进行 CDC 么？日志如何拉取？
支持，是通过订阅 MySQL binlog 日志方式到同步服务器上解析 binlog 日志方式进行

## MySQL CDC 同步需要权限如何开启？
需要有对应库和表的 select 权限。
1、授权语句如下：
```
GRANT SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON . TO 'username'@'host' IDENTIFIED BY 'password';
FLUSH PRIVILEGES;
```
2、编辑 /etc/mysql/my.cnf  添加以下行：
```
[mysqld]
log-bin=/var/log/mysql/mysql-bin.log
expire_logs_days = 7
binlog_format = ROW
binlog_row_image=full
```
3、然后重启 MySQL 服务：
```
service mysql restart
```

## SQLServer CDC 同步需要权限如何开启？
使用 SQLServer CDC 数据源需要 SQLServer 先开启 MS-CDC 功能，具体的操作如下：
1、查询 SQLSERVER CDC Agent 是否已经开启
```
   EXEC xp_servicecontrol N'querystate', N'SQLServerAGENT';
   - 如果结果是running.证明已开启，否则需要手动开启
``` 
2、
2.1、如果是 Linux 开启 SQLSERVER CDC Agent
```
   /opt/mssql/bin/mssql-conf setup
# 返回结果如下
1) Evaluation (free, no production use rights, 180-day limit)
2) Developer (free, no production use rights)
3) Express (free)
4) Web (PAID)
5) Standard (PAID)
6) Enterprise (PAID)
7) Enterprise Core (PAID)
8) I bought a license through a retail sales channel and have a product key to enter.
```

可以根据实际情况来进行选择
可以直接选择 2 免费的，同时包含 agent
选择后设置开启 agent
```
/opt/mssql/bin/mssql-conf set sqlagent.enabled true
```

2.2、如果是 Windows 开启 SQLSERVER Agent (以 SQLServer 2008 为例)
- (1)官方文档： https://learn.microsoft.com/zh-cn/previous-versions/sql/sql-server-2008-r2/ms191454(v=sql.105)?redirectedfrom=MSDN
```
在“开始”菜单中，依次指向“所有程序”、Microsoft SQL Server 2008 R2、“配置工具”，然后单击“SQL Server 配置管理器”**。
在 SQL Server 配置管理器中，展开“服务”，然后单击“SQL 代理”。
在结果窗格中，右键单击任何实例，再单击“启动”。
SQL Server Agent 旁的图标上和工具栏上的绿色箭头指示 SQL Server Agent 已成功启动。
单击“确定”。
```

- (2)启动任务管理器-打开服务，找到SQLServer Agent (MSSQLSERVER)" 服务（其中 "MSSQLSERVER" 可能根据 SQL Server 实例而有所不同），然后启动服务。

3、先设置库级别的CDC (需开通库级别和表级别 CDC)
```
-- 下面设置库级别开启CDC,该级别下，开启的CDC的库下面的所有表自动开启CDC
USE TestDB; -- 替换为实际的数据库名称
EXEC sys.sp_cdc_enable_db;

-- 查询库是否开启CDC
SELECT name, is_cdc_enabled
FROM sys.databases
WHERE name = 'database';-- 替换为要检查的数据库的名称
```
4、然后设置表级别的CDC
```
-- 下面设置表级别开启CDC，该级别下只有显示开启了CDC的表才能进行CDC同步
USE TestDB; -- 替换为实际的数据库名称
EXEC sys.sp_cdc_enable_table
@source_schema = 'dbo',
@source_name = 'table', -- 替换为要启用 CDC 的表的名称
@role_name = NULL,
@capture_instance = 'table'; -- 替换为一个唯一的捕获实例名称

-- 查询表是否开启CDC
USE TestDB; -- 替换为实际的数据库名称

SELECT name, is_tracked_by_cdc
FROM sys.tables
WHERE name = 'table'; -- 替换为要检查的表的名称
```

## 是否支持无主键表的 CDC 同步？
不支持无主键表的 cdc 同步。原因如下：
比如上游有 2 条一模一样的数据，然后上游删除或修改了一条，下游由于无法区分到底是哪条需要删除或修改，会出现这 2 条都被删除或修改的情况。
没主键要类似去重的效果本身有点儿自相矛盾，就像辨别西游记里的真假悟空，到底哪个是真的

## PostgreSql 任务执行时报错：Caused by: org.postgresql.util.PSQLException: ERROR: all replication slots are in use
这是因为 PostgreSql 的 slot 满了，需要手动释放
通过修改 postgresql.conf 文件，将  max_wal_senders、max_replication_slots增加，然后重启 PostgreSql 服务，命令：`systemctl estart postgresql`
1.  max_wal_senders = 1000      # max number of walsender processes
2.  max_replication_slots = 1000     # max number of replication slots


## 我有一个问题，我自己无法解决
我在使用 SeaTunnel 时遇到了问题，无法自行解决。 我应该怎么办？有以下几种方式
1、在[问题列表](https://github.com/apache/seatunnel/issues)或[邮件列表](https://lists.apache.org/list.html?dev@seatunnel.apache.org)中搜索看看是否有人已经问过同样的问题并得到答案。 
2、如果您找不到问题的答案，您可以通过[这些方式](https://github.com/apache/seatunnel#contact-us)联系社区成员寻求帮助。
3、中国用户可以添加微信群助手：seatunnel1，加入社区交流群，也欢迎大家关注微信公众号：seatunnel。

## 如何声明变量？
您想知道如何在 SeaTunnel 的配置中声明一个变量，然后在运行时动态替换该变量的值吗？ 该功能常用于定时或非定时离线处理，以替代时间、日期等变量。 用法如下：
在配置中配置变量名称。 下面是一个sql转换的例子（实际上，配置文件中任何地方“key = value”中的值都可以使用变量替换）：
```
...
transform {
  Sql {
    query = "select * from user_view where city ='${city}' and dt = '${date}'"
  }
}
...
```

以使用 SeaTunnel Zeta Local模式为例，启动命令如下：

```bash
$SEATUNNEL_HOME/bin/seatunnel.sh \
-c $SEATUNNEL_HOME/config/your_app.conf \
-m local[2] \
-i city=shanghai \
-i date=20231110
```

您可以使用参数“-i”或“--variable”后跟“key=value”来指定变量的值，其中key需要与配置中的变量名称相同。详情可以参考：https://seatunnel.apache.org/docs/concept/config

## 如何在配置文件中写入多行文本的配置项？
当配置的文本很长并且想要将其换行时，您可以使用三个双引号来指示其开始和结束：

```
var = """
Apache SeaTunnel is a
next-generation high-performance,
distributed, massive data integration tool.
"""
```

## 如何实现多行文本的变量替换？
在多行文本中进行变量替换有点麻烦，因为变量不能包含在三个双引号中：

```
var = """
your string 1
"""${you_var}""" your string 2"""
```

请参阅：[lightbend/config#456](https://github.com/lightbend/config/issues/456)。

## 如何配置 SeaTunnel-E2E Test 的日志记录相关参数？
`seatunnel-e2e` 的 log4j 配置文件位于 `seatunnel-e2e/seatunnel-e2e-common/src/test/resources/log4j2.properties` 中。 您可以直接在配置文件中修改日志记录相关参数。

例如，如果您想输出更详细的E2E Test日志，只需将配置文件中的“rootLogger.level”降级即可。

## 如果想学习 SeaTunnel 的源代码，应该从哪里开始？
SeaTunnel 拥有完全抽象、结构化的非常优秀的架构设计和代码实现，很多用户都选择 SeaTunnel 作为学习大数据架构的方式。 您可以从`seatunnel-examples`模块开始了解和调试源代码：SeaTunnelEngineLocalExample.java
具体参考：https://seatunnel.apache.org/docs/contribution/setup
针对中国用户，如果有伙伴想贡献自己的一份力量让 SeaTunnel 更好，特别欢迎加入社区贡献者种子群，欢迎添加微信：davidzollo，添加时请注明开源共建。

## 如果想开发自己的 source、sink、transform 时，是否需要了解 SeaTunnel 所有源代码？
不需要，您只需要关注 source、sink、transform 对应的接口即可。
如果你想针对 SeaTunnel API 开发自己的连接器（Connector V2），请查看**[Connector Development Guide](https://github.com/apache/seatunnel/blob/dev/seatunnel-connectors-v2/README.zh.md)** 。


