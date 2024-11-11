# 常见问题解答

## 使用 SeaTunnel 需要安装 Spark 或者 Flink 这样的引擎么？
SeaTunnel 支持 Zeta、Spark 和 Flink 作为同步引擎的选择，您可以选择之一就行，社区尤其推荐使用 Zeta 这种专为同步场景打造的新一代超高性能同步引擎。Zeta 被社区用户亲切的称为 “泽塔奥特曼”! 
社区对 Zeta 的支持力度是最大的，功能也更丰富。

## SeaTunnel 支持哪些数据来源和数据目的地？
SeaTunnel 支持多种数据源来源和数据目的地，您可以在官网找到详细的列表：
SeaTunnel 支持的数据来源(Source)列表：https://seatunnel.apache.org/docs/connector-v2/source
SeaTunnel 支持的数据目的地(Sink)列表：https://seatunnel.apache.org/docs/connector-v2/sink

## SeaTunnel 连接器插件下载缓慢？
SeaTunnel 默认从 Maven 官方仓库下载连接器插件，可能会导致下载速度缓慢。为加速下载，可以修改安装插件的脚本，使用本地的 Maven，并配置国内的 Maven 镜像源，如阿里云的 Maven 源

## 目前支持哪些数据源的 CDC ？
目前支持 MongoDB CDC、MySQL CDC、Opengauss CDC、Oracle CDC、PostgreSQL CDC、Sql Server CDC、TiDB CDC等，更多请查阅[Source](https://seatunnel.apache.org/docs/connector-v2/source)。

## SeaTunnel 支持自动建表么？
不少 SeaTunnel 的 Sink 端已经支持了自动建表，你可以查阅对应的 Sink 的文档，里面有关于 schema_save_mode 参数详细的介绍，sink 参数里有 `schema_save_mode` 就可以支持自动建表。
拿 Jdbc 举例，你可以查阅对应的 Jdbc Sink 的文档[Jdbc Sink](https://seatunnel.apache.org/docs/connector-v2/sink/Jdbc#schema_save_mode-enum)，其中有关于 schema_save_mode 参数详细的介绍，具体如下：
1、**RECREATE_SCHEMA**：当表不存在时会创建，当表已存在时会删除并重建
2、**CREATE_SCHEMA_WHEN_NOT_EXIST**：当表不存在时会创建，当表已存在时则跳过创建
3、**ERROR_WHEN_SCHEMA_NOT_EXIST**：当表不存在时将抛出错误
4、**IGNORE** ：忽略对表的处理

## SeaTunnel 支持同步任务开始前对目标侧已经存在的数据进行处理么？
不少 SeaTunnel 的 Sink 端已经支持了对目标侧数据进行处理，你可以查阅对应的 Sink 的文档，里面有关于 data_save_mode 参数详细的介绍，sink 参数里有 `data_save_mode` 就可以支持。
拿 Jdbc 举例，你可以查阅对应的 Jdbc Sink 的文档[Jdbc Sink](https://seatunnel.apache.org/docs/2.3.8/connector-v2/sink/Jdbc#data_save_mode-enum)，其中有关于 `data_save_mode` 参数详细的介绍，具体如下：
1、**DROP_DATA**：保留数据库结构，删除数据
2、**APPEND_DATA**：保留数据库结构，保留数据
3、**CUSTOM_PROCESSING**：允许用户自定义数据处理方式
4、**ERROR_WHEN_DATA_EXISTS**：当有数据时抛出错误

## 支持从 MySQL 备库进行 CDC 么？日志如何拉取？
支持，是通过订阅 MySQL binlog 日志方式到同步服务器上解析 binlog 日志方式进行

## 是否支持无主键表的 CDC 同步？
不支持无主键表的 cdc 同步。原因如下：
比如上游有 2 条一模一样的数据，然后上游删除或修改了一条，下游由于无法区分到底是哪条需要删除或修改，会出现这 2 条都被删除或修改的情况。
没主键要类似去重的效果本身有点儿自相矛盾，就像辨别西游记里的真假悟空，到底哪个是真的

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

您可以使用参数“-i”或“--variable”后跟“key=value”来指定变量的值，其中key需要与配置中的变量名称相同。详情可以参考：https://seatunnel.apache.org/docs/concept/config/#config-variable-substitution

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


