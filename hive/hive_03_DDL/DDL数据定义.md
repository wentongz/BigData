#                   DDL数据定义 

#### 创建数据库

```
CREATE DATABASE [IF NOT EXISTS] database_name
[COMMENT database_comment]
[LOCATION hdfs_path]
[WITH DBPROPERTIES (property_name=property_value, ...)];
```

1）创建一个数据库，数据库在HDFS上的默认存储路径是/user/hive/warehouse/*.db。

```
hive (default)> create database db_hive;
```

2）避免要创建的数据库已经存在错误，增加if not exists判断。（标准写法）

```
hive (default)> create database db_hive;
FAILED: Execution Error, return code 1 from org.apache.hadoop.hive.ql.exec.DDLTask. Database db_hive already exists
hive (default)> create database if not exists db_hive;
```

3）创建一个数据库，指定数据库在HDFS上存放的位置

```
hive (default)> create database db_hive2 location '/db_hive2.db';
```

![1596469793062](D:\北大青鸟\大数据\hadoop\Hive\hive_03_DDL\1596469793062.png)

## ***\*4.2 查询数据库\****

### ***\*4.2.1 显示数据库\****

1．显示数据库

```
hive> show databases;
```

2．过滤显示查询的数据库

```
hive> show databases like 'db_hive*';

OK
db_hive
db_hive_1
```



### ***\*4.2.2 查看数据库详情\****

1．显示数据库信息

```
hive> desc database db_hive;
OK
db_hive		hdfs://hadoop102:9000/user/hive/warehouse/db_hive.db	rootUSER	
```

2．显示数据库详细信息，extended

```
hive> desc database extended db_hive;
OK
db_hive		hdfs://hadoop102:9000/user/hive/warehouse/db_hive.db	rootUSER
```

​	

#### 4.2.3 切换当前数据库

```
hive (default)> use db_hive;
```



#### 4.3 修改数据库

用户可以使用ALTER DATABASE命令为某个数据库的DBPROPERTIES设置键-值对属性值，来描述这个数据库的属性信息。数据库的其他元数据信息都是不可更改的，包括数据库名和数据库所在的目录位置。

hive (default)> alter database db_hive set dbproperties('createtime'='20170830');

在hive中查看修改结果

hive> desc database extended db_hive;

db_name comment location     owner_name    owner_type    parameters

db_hive     hdfs://hadoop102:8020/user/hive/warehouse/db_hive.db   root USER   {createtime=20170830}

#### *4.4 删除数据库*

1．删除空数据库

```
hive>drop database db_hive2;
```

2．如果删除的数据库不存在，最好采用 if exists判断数据库是否存在

```
hive> drop database db_hive;
```

FAILED: SemanticException [Error 10072]: Database does not exist: db_hive

```
hive> drop database if exists db_hive2;
```

3．如果数据库不为空，可以采用cascade命令，强制删除

```
hive> drop database db_hive;
```

FAILED: Execution Error, return code 1 from org.apache.hadoop.hive.ql.exec.DDLTask. InvalidOperationException(message:Database db_hive is not empty. One or more tables exist.)

```
hive> drop database db_hive cascade;
```

#### 4.5 创建表

1．建表语法

CREATE [EXTERNAL] TABLE [IF NOT EXISTS] table_name 

[(col_name data_type [COMMENT col_comment], ...)] 

[COMMENT table_comment] 

[PARTITIONED BY (col_name data_type [COMMENT col_comment], ...)] 

[CLUSTERED BY (col_name, col_name, ...) 

[SORTED BY (col_name [ASC|DESC], ...)] INTO num_buckets BUCKETS] 

[ROW FORMAT row_format] 

[STORED AS file_format] 

[LOCATION hdfs_path]

[TBLPROPERTIES (property_name=property_value, ...)]

[AS select_statement]

2．字段解释说明 

（1）CREATE TABLE 创建一个指定名字的表。如果相同名字的表已经存在，则抛出异常；用户可以用 IF NOT EXISTS 选项来忽略这个异常。

（2）EXTERNAL关键字可以让用户创建一个外部表，在建表的同时可以指定一个指向实际数据的路径（LOCATION），在删除表的时候，内部表的元数据和数据会被一起删除，而外部表只删除元数据，不删除数据。

（3）COMMENT：为表和列添加注释。

（4）PARTITIONED BY创建分区表

（5）CLUSTERED BY创建分桶表

（6）SORTED BY不常用，对桶中的一个或多个列另外排序

（7）ROW FORMAT 

DELIMITED [FIELDS TERMINATED BY char] [COLLECTION ITEMS TERMINATED BY char]

​    [MAP KEYS TERMINATED BY char] [LINES TERMINATED BY char] 

  | SERDE serde_name [WITH SERDEPROPERTIES (property_name=property_value, property_name=property_value, ...)]

用户在建表的时候可以自定义SerDe或者使用自带的SerDe。如果没有指定ROW FORMAT 或者ROW FORMAT DELIMITED，将会使用自带的SerDe。在建表的时候，用户还需要为表指定列，用户在指定表的列的同时也会指定自定义的SerDe，Hive通过SerDe确定表的具体的列的数据。

SerDe是Serialize/Deserilize的简称， hive使用Serde进行行对象的序列与反序列化。

（8）STORED AS指定存储文件类型

常用的存储文件类型：SEQUENCEFILE（二进制序列文件）、TEXTFILE（文本）、RCFILE（列式存储格式文件）

如果文件数据是纯文本，可以使用STORED AS TEXTFILE。如果数据需要压缩，使用 STORED AS SEQUENCEFILE。

（9）LOCATION ：指定表在HDFS上的存储位置。

（10）AS：后跟查询语句，根据查询结果创建表。

（11）LIKE允许用户复制现有的表结构，但是不复制数据。

#### 4.5.1 管理表

1．理论

默认创建的表都是所谓的管理表，有时也被称为内部表。因为这种表，Hive会（或多或少地）控制着数据的生命周期。Hive默认情况下会将这些表的数据存储在由配置项hive.metastore.warehouse.dir(例如，/user/hive/warehouse)所定义的目录的子目录下。	当我们删除一个管理表时，Hive也会删除这个表中数据。管理表不适合和其他工具共享数据。

2．案例实操

（1）普通创建表

```
create table if not exists student2(
id int, name string
)
row format delimited fields terminated by '\t'
stored as textfile
location '/user/hive/warehouse/student2';
```

（2）根据查询结果创建表（查询的结果会添加到新创建的表中）

```
create table if not exists student3 as select id, name from student;
```

（3）根据已经存在的表结构创建表

```
create table if not exists student4 like student;
```

（4）查询表的类型

hive (default)> desc formatted student2;

Table Type:       MANAGED_TABLE  

### 4.5.2 外部表

1．理论

因为表是外部表，所以Hive并非认为其完全拥有这份数据。删除该表并不会删除掉这份数据，不过描述表的元数据信息会被删除掉。

2．管理表和外部表的使用场景

每天将收集到的网站日志定期流入HDFS文本文件。在外部表（原始日志表）的基础上做大量的统计分析，用到的中间表、结果表使用内部表存储，数据通过SELECT+INSERT进入内部表。

3．案例实操

分别创建部门和员工外部表，并向表中导入数据。

（1） 上传数据到HDFS

```
hive (default)> dfs -mkdir /student;
hive (default)> dfs -put /opt/module/hive/datas/student.txt /student;
```

​	（2）建表语句

​		创建外部表

```
hive (default)> create external table stu_external(
id int, 
name string) 
row format delimited fields terminated by '\t' 
location '/student';
```

​	（3）查看创建的表

hive (default)> select * from stu_external;

OK

stu_external.id stu_external.name

1001   lisi

1002   wangwu

1003   zhaoliu

​	（4）查看表格式化数据

hive (default)> desc formatted stu_external;

Table Type:       EXTERNAL_TABLE

​	（5）删除外部表

hive (default)> drop table stu_external;

外部表删除后，hdfs中的数据还在，但是metadata中stu_external的元数据已被删除

### 4.5.3 管理表与外部表的互相转换

（1）查询表的类型

hive (default)> desc formatted student2;

Table Type:       MANAGED_TABLE

（2）修改内部表student2为外部表

alter table student2 set tblproperties('EXTERNAL'='TRUE');

（3）查询表的类型

hive (default)> desc formatted student2;

Table Type:       EXTERNAL_TABLE

（4）修改外部表student2为内部表

alter table student2 set tblproperties('EXTERNAL'='FALSE');

（5）查询表的类型

hive (default)> desc formatted student2;

Table Type:       MANAGED_TABLE

注意：('EXTERNAL'='TRUE')和('EXTERNAL'='FALSE')为固定写法，区分大小写！

## ***\*4.7 修改表\****

### ***\*4.7.1 重命名表\****

1．语法

ALTER TABLE table_name RENAME TO new_table_name

2．实操案例

hive (default)> alter table dept_partition2 rename to dept_partition3;

### ***\*4.7.3 增加/修改/替换列信息\****

1．语法

​	更新列

ALTER TABLE table_name CHANGE [COLUMN] col_old_name col_new_name column_type [COMMENT col_comment] [FIRST|AFTER column_name]

增加和替换列

ALTER TABLE table_name ADD|REPLACE COLUMNS (col_name data_type [COMMENT col_comment], ...) 

注：ADD是代表新增一字段，字段位置在所有列后面(partition列前)，REPLACE则是表示替换表中所有字段。

2．实操案例

（1）查询表结构

hive> desc dept_partition;

（2）添加列

hive (default)> alter table dept_partition add columns(deptdesc string);

（3）查询表结构

hive> desc dept_partition;

（4）更新列

hive (default)> alter table dept_partition change column deptdesc desc int;

（5）查询表结构

hive> desc dept_partition;

（6）替换列

hive (default)> alter table dept_partition replace columns(deptno string, dname

 string, loc string);

（7）查询表结构

hive> desc dept_partition;

## ***\*4.8 删除表\****

hive (default)> drop table dept_partition;

