# !!!DO NOT USE IN PRODUCTION!!!
This project is an expirenment and is not meant for production.
The project structure can change a lot.

# Database migration
## Goal
The goals of this project:

- Create a database agnostic migration tool
- Configuration created with java code 

This is only meant for simple database structures. It will not support database views, functions or procedures.

## Supported databases

- H2
- HyperSQL
- MySQL
- PostgreSQL

## Examples

### Create table
```java
Migration.Builder builder = new Migration.Builder();

builder.table("some_table")
    .addColumn("id", Column.TYPE.INTEGER, column -> column.primary(true).autoIncrement(true))
    .addColumn("name", Column.TYPE.VARCHAR);
```

### Create table with foreign keys
```java
Migration.Builder builder = new Migration.Builder();

builder.table("some_other_table")
    .addColumn("id", Column.TYPE.INTEGER, column -> column.primary(true).autoIncrement(true))
    .addColumn("some_table_id", Column.TYPE.INTEGER)
    .foreignKey("some_table", "some_table_id", "id", key -> {
        key.cascadeDelete(ForeignKey.CASCADE.RESTRICT);
        key.cascadeUpdate(ForeignKey.CASCADE.RESTRICT);
    });
```

### Rename column
```java
Migration.Builder builder = new Migration.Builder();

builder.table("some_table")
    .changeColumn("name", column -> column.rename("renamed"));
```

### Change column type
```java
Migration.Builder builder = new Migration.Builder();

builder.table("some_table")
    .changeColumn("name", column -> column.type(Column.TYPE.VARCHAR));
```

### Column type size
```java
Migration.Builder builder = new Migration.Builder();

builder.table("some_table")
    .addColumn("name", Column.TYPE.VARCHAR, column -> column.size(25));
```

## Requirements

- Java 8+
- Docker