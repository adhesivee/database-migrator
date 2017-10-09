# Database migration
## Goal
The goals of this project:

- Create a database agnostic migration tool
- Configuration created with java code 

This is only meant for simple database structures. It will not support database views, functions or procedures.

## Maven
```xml
<dependency>
    <groupId>nl.myndocs</groupId>
    <artifactId>database-migrator</artifactId>
    <version>1.0.0-beta4</version>
</dependency>
```
## Features
### Supported databases

- Derby
- H2
- HyperSQL
- MySQL
- PostgreSQL

### Supported column types

- BOOLEAN
- VARCHAR
- CHAR
- SMALLINT
- INT 
- BIGINT
- UUID
- DATE
- TIME
- TIMESTAMP

### Supported cascading types

- RESTRICT
- SET_NULL
- SET_DEFAULT
- NO_ACTION
- CASCADE
                
## Examples
### Migrations 
#### Creating migration script
```java
public class FirstMigrationScript implements MigrationScript {
    @Override
    public String migrationId() {
        // Make sure the id is unique over all the migration scripts
        return "MIGRATION-1";
    }

    @Override
    public void migrate(Migration migration) {
        migration.table("some_table")
            .addColumn("id", Column.TYPE.INTEGER, column -> column.primary(true).autoIncrement(true))
            .addColumn("name", Column.TYPE.VARCHAR)
            .save();
    }
}
```
#### Bootstrapping
```java
// Get the JDBC connection
Connection connection = ...;
Database database =  Selector().loadFromConnection(connection);
Migrator migration = new Migrator(database);
migration.migrate(
        new FirstMigrationScript(),
        new SecondMigrationScript()
)
```

Migrations will be executed in the same order on how it is passed to `.migrate()`

### Migration capabilities
#### Create table
```java
migration.table("some_table")
    .addColumn("id", Column.TYPE.INTEGER, column -> column.primary(true).autoIncrement(true))
    .addColumn("name", Column.TYPE.VARCHAR)
    .save();
```

#### Create table with foreign keys
```java
migration.table("some_other_table")
    .addColumn("id", Column.TYPE.INTEGER, column -> column.primary(true).autoIncrement(true))
    .addColumn("some_table_id", Column.TYPE.INTEGER)
    .addForeignKey("some_FK", "some_table", "some_table_id", "id", key -> {
        key.cascadeDelete(ForeignKey.CASCADE.RESTRICT);
        key.cascadeUpdate(ForeignKey.CASCADE.RESTRICT);
    })
    .save();
```

#### Rename column
```java
migration.table("some_table")
    .changeColumn("name", column -> column.rename("renamed"))
    .save();
```

#### Change column type
```java
migration.table("some_table")
    .changeColumn("name", column -> column.type(Column.TYPE.VARCHAR));
```

#### Column type size
```java
migration.table("some_table")
    .addColumn("name", Column.TYPE.VARCHAR, column -> column.size(25));
```

### Getting the JDBC connection
```java
// Do not close the connection!
Connection connection = migration.getDatabase().getConnection();
```

## Requirements

### Runtime
- Java 8+

### Test
- Docker