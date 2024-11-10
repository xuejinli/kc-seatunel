# Frequently Asked Questions

## Do I need to install engines like Spark or Flink to use SeaTunnel?
No, SeaTunnel supports Zeta, Spark, and Flink as options for the integration engine. You can choose one of them. The community especially recommends using Zeta, a new-generation high-performance engine specifically built for integration scenarios.
The community provides the most support for Zeta, which also has richer features.

## What data sources and destinations does SeaTunnel support?
SeaTunnel supports a variety of data sources and destinations. You can find the detailed list on the official website:
- Supported data sources (Source): https://seatunnel.apache.org/docs/connector-v2/source
- Supported data destinations (Sink): https://seatunnel.apache.org/docs/connector-v2/sink

## Which data sources currently support CDC (Change Data Capture)?
Currently, CDC is supported for MongoDB CDC, MySQL CDC, OpenGauss CDC, Oracle CDC, PostgreSQL CDC, SQL Server CDC, TiDB CDC, etc. For more details, refer to the [Source](https://seatunnel.apache.org/docs/connector-v2/source) documentation.

## Does it support CDC from MySQL replica? How is the log fetched?
Yes, it is supported by subscribing to the MySQL binlog and parsing the binlog on the synchronization server.

## What permissions are required for MySQL CDC synchronization and how to enable them?
You need `SELECT` permission on the relevant databases and tables.
1. The authorization statement is as follows:
```
GRANT SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'username'@'host' IDENTIFIED BY 'password';
FLUSH PRIVILEGES;
```

2. Edit `/etc/mysql/my.cnf` and add the following lines:
```
[mysqld]
log-bin=/var/log/mysql/mysql-bin.log
expire_logs_days = 7
binlog_format = ROW
binlog_row_image=full
```

3. Restart the MySQL service:
```
service mysql restart
```

## What permissions are required for SQL Server CDC synchronization and how to enable them?
Using SQL Server CDC as a data source requires enabling the MS-CDC feature in SQL Server. The steps are as follows:

1. Check if the SQL Server CDC Agent is running:
```
EXEC xp_servicecontrol N'querystate', N'SQLServerAGENT';
-- If the result is "running," it means the agent is enabled. Otherwise, it needs to be started manually.
```

2. If using Linux, enable the SQL Server CDC Agent:
```
/opt/mssql/bin/mssql-conf setup
The result that is returned is as follows:
1) Evaluation (free, no production use rights, 180-day limit)
2) Developer (free, no production use rights)
3) Express (free)
4) Web (PAID)
5) Standard (PAID)
6) Enterprise (PAID)
7) Enterprise Core (PAID)
8) I bought a license through a retail sales channel and have a product key to enter.
```
Choose the appropriate option based on your situation.
Select option 2 (Developer) for a free version that includes the agent. Enable the agent by running:
```
/opt/mssql/bin/mssql-conf set sqlagent.enabled true
```

If using Windows, enable SQL Server Agent (e.g., for SQL Server 2008):
   - Refer to the [official documentation](https://learn.microsoft.com/en-us/previous-versions/sql/sql-server-2008-r2/ms191454(v=sql.105)).
```
Open "SQL Server Configuration Manager" from the Start menu, navigate to "SQL Server Services," right-click the "SQL Server Agent" instance, and start it.
```

3. Firstly, enable CDC at the database level:
```
USE TestDB; -- Replace with your actual database name
EXEC sys.sp_cdc_enable_db;

-- Check if the database has CDC enabled
SELECT name, is_cdc_enabled
FROM sys.databases
WHERE name = 'database'; -- Replace with the name of your database
```

4. Secondly, enable CDC at the table level:
```
USE TestDB; -- Replace with your actual database name
EXEC sys.sp_cdc_enable_table
@source_schema = 'dbo',
@source_name = 'table', -- Replace with the table name
@role_name = NULL,
@capture_instance = 'table'; -- Replace with a unique capture instance name

-- Check if the table has CDC enabled
SELECT name, is_tracked_by_cdc
FROM sys.tables
WHERE name = 'table'; -- Replace with the table name
```

## Does SeaTunnel support CDC synchronization for tables without primary keys?
No, CDC synchronization is not supported for tables without primary keys. This is because, if there are two identical rows upstream and one is deleted or modified, it would be impossible to distinguish which row should be deleted or modified downstream, potentially resulting in both rows being affected. 

## Error during PostgreSQL task execution: Caused by: org.postgresql.util.PSQLException: ERROR: all replication slots are in use
This error occurs when the replication slots in PostgreSQL are full and need to be released. Modify the `postgresql.conf` file to increase `max_wal_senders` and `max_replication_slots`, then restart the PostgreSQL service using the command:
```
systemctl restart postgresql
```
Example configuration:
```
max_wal_senders = 1000      # max number of walsender processes
max_replication_slots = 1000     # max number of replication slots
```

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
Use the `-i` or `--variable` parameter followed by `key=value` to specify the variable's value, ensuring that `key` matches the variable name in the configuration. For more details, refer to: https://seatunnel.apache.org/docs/concept/config

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
