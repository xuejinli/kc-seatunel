# Explode

> Explode transform plugin

## Description

将一行数据按照指定的分割符拆分成多条数据。

## Options

|      name      | type | required |
|----------------|------|----------|
| explode_string_fields | Map  | yes      |
| explode_list_fields | List | yes      |


### explode_string_fields [Map]

需要按照指定分隔符分割的字段。
map是由field作为key，分隔符作为value。


### explode_list_fields [List]

需要按照指定分隔符分割的字段。
List为需要切分的字段。


### common options [config]

转换插件的常见参数, 请参考  [Transform Plugin](common-options.md) 了解详情

## 示例

源端数据读取的表格如下：

The data read from source is a table like this:

| name              | age | card    |
|-------------------|-----|---------|
| Joy Ding,May Ding | 20  | [123,234]  |
| Kin Dom,Joy Dom   | 20  | [123,345] |

当我们想根据name和card进行数据拆分：

```
transform {
  Explode {
    source_table_name = "fake"
    result_table_name = "fake1"
    explode_string_fields = {"name":","}
    explode_list_fields = ["card"]
  }
}
```

那么结果表 `fake1` 中的数据将会像这样：

|   name   | age | card |
|----------|-----|------|
| Joy Ding | 20  | 123  |
| Joy Ding | 20  | 234  |
| May Ding | 20   | 123  |
| May Ding | 20   | 234  |
| Kin Dom  | 20  | 123  |
| Kin Dom  | 20  | 345  |
| Joy Dom  | 20  | 123  |
| Joy Dom  | 20  | 345  |

## Changelog

### new version

- Add Explode Transform Connector

