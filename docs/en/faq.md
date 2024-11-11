# Frequently Asked Questions

## Do I need to install engines like Spark or Flink to use SeaTunnel?
SeaTunnel supports Zeta, Spark, and Flink as options for the integration engine. You can choose one of them. The community especially recommends using Zeta, a new-generation high-performance engine specifically built for integration scenarios.
The community provides the most support for Zeta, which also has richer features.

## What data sources and destinations does SeaTunnel support?
SeaTunnel supports a variety of data sources and destinations. You can find the detailed list on the official website:
- Supported data sources (Source): https://seatunnel.apache.org/docs/connector-v2/source
- Supported data destinations (Sink): https://seatunnel.apache.org/docs/connector-v2/sink

## Which data sources currently support CDC (Change Data Capture)?
Currently, CDC is supported for MongoDB CDC, MySQL CDC, OpenGauss CDC, Oracle CDC, PostgreSQL CDC, SQL Server CDC, TiDB CDC, etc. For more details, refer to the [Source](https://seatunnel.apache.org/docs/connector-v2/source) documentation.

## Does SeaTunnel Support Automatic Table Creation?
Many of SeaTunnel’s Sink connectors support automatic table creation. You can refer to the documentation for each specific Sink connector to find details about the `schema_save_mode` parameter. If the `schema_save_mode` parameter is available in the sink configuration, automatic table creation is supported.
Using the Jdbc Sink as an example, you can refer to the [Jdbc Sink documentation](https://seatunnel.apache.org/docs/2.3.8/connector-v2/sink/Jdbc/#schema_save_mode-enum), which provides detailed information on the `schema_save_mode` parameter. Specifically:

1. **RECREATE_SCHEMA**: Creates the table if it doesn’t exist; if it does exist, the table is dropped and recreated.
2. **CREATE_SCHEMA_WHEN_NOT_EXIST**: Creates the table if it doesn’t exist; skips creation if the table already exists.
3. **ERROR_WHEN_SCHEMA_NOT_EXIST**: Throws an error if the table doesn’t exist.
4. **IGNORE**: Ignores any table-related handling.

## Does SeaTunnel Support Handling Existing Data on the Target Side Before Syncing?
Many of SeaTunnel’s Sink connectors support handling existing data on the target side. You can refer to the specific Sink connector’s documentation, which includes details on the `data_save_mode` parameter. If the `data_save_mode` parameter is available in the sink configuration, target data handling is supported.
Using the Jdbc Sink as an example, you can refer to the [Jdbc Sink documentation](https://seatunnel.apache.org/docs/2.3.8/connector-v2/sink/Jdbc/#data_save_mode-enum), which provides detailed information on the `data_save_mode` parameter. Specifically:

1. **DROP_DATA**: Retains the database structure but deletes data.
2. **APPEND_DATA**: Retains both the database structure and the existing data.
3. **CUSTOM_PROCESSING**: Allows custom data handling as defined by the user.
4. **ERROR_WHEN_DATA_EXISTS**: Throws an error if data already exists.

## Does it support CDC from MySQL replica? How is the log fetched?
Yes, it is supported by subscribing to the MySQL binlog and parsing the binlog on the synchronization server.

## Does SeaTunnel support CDC synchronization for tables without primary keys?
No, CDC synchronization is not supported for tables without primary keys. This is because, if there are two identical rows upstream and one is deleted or modified, it would be impossible to distinguish which row should be deleted or modified downstream, potentially resulting in both rows being affected.

## What should I do if I have a problem that I can't solve on my own?
If you encounter an issue while using SeaTunnel that you cannot resolve, you can:
1. Search the [issue list](https://github.com/apache/seatunnel/issues) or [mailing list](https://lists.apache.org/list.html?dev@seatunnel.apache.org) to see if someone else has asked the same question and received an answer.
2. If you can't find an answer, reach out to the community for help using [these methods](https://github.com/apache/seatunnel#contact-us).

## How do I declare variables?
Do you want to know how to declare a variable in a SeaTunnel configuration and dynamically replace its value at runtime? This feature is often used in both scheduled and non-scheduled offline processing as a placeholder for variables such as time and date. Here’s how to do it:
Declare a variable name in the configuration. Below is an example of a SQL transformation (in fact, any value in `key = value` format can use variable substitution):
```
...
transform {
  Sql {
    query = "select * from user_view where city ='${city}' and dt = '${date}'"
  }
}
...
```
To run SeaTunnel in Zeta Local mode, use the following command:
```bash
$SEATUNNEL_HOME/bin/seatunnel.sh \
-c $SEATUNNEL_HOME/config/your_app.conf \
-m local[2] \
-i city=shanghai \
-i date=20231110
```
Use the `-i` or `--variable` parameter followed by `key=value` to specify the variable's value, ensuring that `key` matches the variable name in the configuration. For more details, refer to: https://seatunnel.apache.org/docs/concept/config/#config-variable-substitution

## How do I write a multi-line text configuration in the configuration file?
To break a long text into multiple lines, use triple double quotes to indicate the start and end:
```
var = """
Apache SeaTunnel is a
next-generation high-performance,
distributed, massive data integration tool.
"""
```

## How can I perform variable substitution in multi-line text?
Variable substitution in multi-line text is tricky, as variables cannot be enclosed within triple double quotes:
```
var = """
your string 1
"""${you_var}""" your string 2"""
```
See the issue for more information: [lightbend/config#456](https://github.com/lightbend/config/issues/456).

## How do I configure logging parameters for SeaTunnel E2E tests?
The `seatunnel-e2e` log4j configuration file is located at `seatunnel-e2e/seatunnel-e2e-common/src/test/resources/log4j2.properties`. You can modify logging parameters directly in the configuration file.

For example, to get more detailed E2E test logs, simply downgrade the `rootLogger.level` in the configuration file.

## Where should I start if I want to study SeaTunnel's source code?
SeaTunnel has a well-abstracted and structured architecture design and code implementation, making it an excellent choice for learning big data architecture. You can start exploring and debugging the source code from the `seatunnel-examples` module: `SeaTunnelEngineLocalExample.java`. For more details, refer to: https://seatunnel.apache.org/docs/contribution/setup

## If I want to develop my own source, sink or transform program, do I need to understand all of SeaTunnel's source code?
No, you only need to focus on the interfaces related to source, sink, and transform. If you'd like to develop your own connector using SeaTunnel's API (Connector V2), refer to **[Connector Development Guide](https://github.com/apache/seatunnel/blob/dev/seatunnel-connectors-v2/README.md)**.
