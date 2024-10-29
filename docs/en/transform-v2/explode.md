# Explode

> Explode transform plugin

## Description

Split a row of data into multiple data according to the specified delimiter.

## Options

|      name      | type | required | Description   |
|----------------|------|----------|---------------|
| explode_fields | Map  | yes      | Explode field |


### explode_fields [Map]

The fields that need to be separated by the specified delimiter
The map is composed of field as key and separator as value.

### common options [string]

Transform plugin common parameters, please refer to [Transform Plugin](common-options.md) for details

## Example

The data read from source is a table like this:

| name              | age | card    |
|-------------------|-----|---------|
| Joy Ding,May Ding | 20  | 123;234 |
| Kin Dom,Joy Dom   | 20  | 123;345 |

we want to explode name and card, we can add a `Explode` Transform like below:

```
transform {
  Explode {
    source_table_name = "fake"
    result_table_name = "fake1"
    explode_fields = {"name":",","card":";"}
  }
}
```


It is useful when you want to explode row.

Then the data in result table `fake1` will like this

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

