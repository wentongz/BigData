### 							  Hive数据类型

#### 基本数据类型



   ![1596468491004](D:\北大青鸟\大数据\hadoop\Hive\hive_02\1596468491004.png)

对于Hive的String类型相当于数据库的varchar类型，该类型是一个可变的字符串，不过它不能声明其中最多能存储多少个字符，理论上它可以存储2GB的字符数。

#### 集合数据类型

![1596468542250](D:\北大青鸟\大数据\hadoop\Hive\hive_02\1596468542250.png)

Hive有三种复杂数据类型ARRAY、MAP 和 STRUCT。ARRAY和MAP与Java中的Array和Map类似，而STRUCT与C语言中的Struct类似，它封装了一个命名字段集合，复杂数据类型允许任意层次的嵌套。



案例实操

1） 假设某表有如下一行，我们用JSON格式来表示其数据结构。在Hive下访问的格式为

```
{
    "name": "songsong",
    "friends": ["bingbing" , "lili"] ,       //列表Array, 
    "children": {                      //键值Map,
        "xiao song": 18 ,
        "xiaoxiao song": 19
    }
    "address": {                      //结构Struct,
        "street": "hui long guan" ,
        "city": "beijing" 
    }
}
```

2）基于上述数据结构，我们在Hive里创建对应的表，并导入数据。 

创建本地测试文件test.txt

```
songsong,bingbing_lili,xiao song:18_xiaoxiao song:19,hui long guan_beijing
yangyang,caicai_susu,xiao yang:18_xiaoxiao yang:19,chao yang_beijing
```

注意：MAP，STRUCT和ARRAY里的元素间关系都可以用同一个字符表示，这里用“_”。

3）Hive上创建测试表test

```
create table test(
name string,
friends array<string>,
children map<string, int>,
address struct<street:string, city:string>
)
row format delimited fields terminated by ','
collection items terminated by '_'
map keys terminated by ':'
lines terminated by '\n';
```

字段解释：

row format delimited fields terminated by ','  -- 列分隔符

collection items terminated by '_'  --MAP STRUCT 和 ARRAY 的分隔符(数据分割符号)

map keys terminated by ':'				-- MAP中的key与value的分隔符

lines terminated by '\n';					-- 行分隔符

4）导入文本数据到测试表

```
hive (default)> load data local inpath '/opt/module/datas/test.txt' into table test;
```

5）访问三种集合列里的数据，以下分别是ARRAY，MAP，STRUCT的访问方式

```
hive (default)> select friends[1],children['xiao song'],address.city from test
where name="songsong";
OK
_c0     _c1     city
lili    18      beijing
Time taken: 0.076 seconds, Fetched: 1 row(s)
```

#### 类型转化

Hive的原子数据类型是可以进行隐式转换的，类似于Java的类型转换，例如某表达式使用INT类型，TINYINT会自动转换为INT类型，但是Hive不会进行反向转化，例如，某表达式使用TINYINT类型，INT不会自动转换为TINYINT类型，它会返回错误，除非使用CAST操作。

1．隐式类型转换规则如下

（1）任何整数类型都可以隐式地转换为一个范围更广的类型，如TINYINT可以转换成INT，INT可以转换成BIGINT。

（2）所有整数类型、FLOAT和STRING类型都可以隐式地转换成DOUBLE。

（3）TINYINT、SMALLINT、INT都可以转换为FLOAT。

（4）BOOLEAN类型不可以转换为任何其它的类型。

2．可以使用CAST操作显示进行数据类型转换

例如CAST('1' AS INT)将把字符串'1' 转换成整数1；如果强制类型转换失败，如执行CAST('X' AS INT)，表达式返回空值 NULL。

```
select '1'+2, cast('1'as int) + 2;
+------+------+--+
| _c0  | _c1  |
+------+------+--+
| 3.0  | 3    |
+------+------+--+
```

