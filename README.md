# Database migration
## Goal
The goals of this project:

- Create a database agnostic migration tool
- Configuration created with java code 

This is only meant for simple database structures. It will not support database views, functions or procedures.

## Features
### Supported databases

- Derby
- H2
- HyperSQL
- MySQL
- PostgreSQL

### Supported column types

- VARCHAR
- CHAR
- INT 
- UUID

### Supported cascading types

- RESTRICT
- SET_NULL
- SET_DEFAULT
- NO_ACTION
- CASCADE
                
## Examples

### Create table
```java
Migration.Builder builder = new Migration.Builder("migration-id");

builder.table("some_table")
    .addColumn("id", Column.TYPE.INTEGER, column -> column.primary(true).autoIncrement(true))
    .addColumn("name", Column.TYPE.VARCHAR);
```

### Create table with foreign keys
```java
Migration.Builder builder = new Migration.Builder("migration-id");

builder.table("some_other_table")
    .addColumn("id", Column.TYPE.INTEGER, column -> column.primary(true).autoIncrement(true))
    .addColumn("some_table_id", Column.TYPE.INTEGER)
    .addForeignKey("some_FK", "some_table", "some_table_id", "id", key -> {
        key.cascadeDelete(ForeignKey.CASCADE.RESTRICT);
        key.cascadeUpdate(ForeignKey.CASCADE.RESTRICT);
    });
```

### Rename column
```java
Migration.Builder builder = new Migration.Builder("migration-id");

builder.table("some_table")
    .changeColumn("name", column -> column.rename("renamed"));
```

### Change column type
```java
Migration.Builder builder = new Migration.Builder("migration-id");

builder.table("some_table")
    .changeColumn("name", column -> column.type(Column.TYPE.VARCHAR));
```

### Column type size
```java
Migration.Builder builder = new Migration.Builder("migration-id");

builder.table("some_table")
    .addColumn("name", Column.TYPE.VARCHAR, column -> column.size(25));
```

## Requirements

### Runtime
- Java 8+

### Test
- Docker