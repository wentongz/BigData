### 			             Hive实战之炒鸡影音

#### 1.1 需求描述

统计硅谷影音视频网站的常规指标，各种TopN指标：

--统计视频观看数Top10

--统计视频类别热度Top10

--统计视频观看数Top20所属类别

--统计视频观看数Top50所关联视频的所属类别Rank

--统计每个类别中的视频热度Top10

--统计每个类别中视频流量Top10

--统计上传视频最多的用户Top10以及他们上传的视频

--统计每个类别视频观看数Top10



#### 2 项目

##### 2.1 数据结构

1．视频表



![1596614838384](G:\beidaqingniao\bigdata\Hive\hive_07_影音统计\1596614838384.png)

2．用户表

![1596614854857](G:\beidaqingniao\bigdata\Hive\hive_07_影音统计\1596614854857.png)



##### 2.2 ETL原始数据

通过观察原始数据形式，可以发现，视频可以有多个所属分类，每个所属分类用&符号分割，且分割的两边有空格字符，同时相关视频也是可以有多个元素，多个相关视频又用“\t”进行分割。为了分析数据时方便对存在多个子元素的数据进行操作，我们首先进行数据重组清洗操作。即：将所有的类别用“&”分割，同时去掉两边空格，多个相关视频id也使用“&”进行分割。

1．ETL之Mapper

```
package com.kgc.etl;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ETLMapper extends Mapper<LongWritable, Text, Text, NullWritable>
{
    private Text k = new Text();

    private StringBuilder sb = new StringBuilder();

    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException
    {
        String line = value.toString();

        String result = handleLine(line);

        if (result == null) {
            context.getCounter("ETL", "False").increment(1L);
        } else {
            context.getCounter("ETL", "True").increment(1L);
            this.k.set(result);
            context.write(this.k, NullWritable.get());
        }
    }

    private String handleLine(String line)
    {
        String[] fields = line.split("\t");
        if (fields.length < 9) {
            return null;
        }
	
        this.sb.delete(0, this.sb.length());
		//People & Blogs   =》  People&Blogs
        fields[3] = fields[3].replace(" ", "");

        for (int i = 0; i < fields.length; i++) {
            if (i == fields.length - 1)
                this.sb.append(fields[i]);
            else if (i < 9)
                this.sb.append(fields[i]).append("\t");
            else {
                this.sb.append(fields[i]).append("&");
            }
        }

        return this.sb.toString();
    }
}
```

2.Driver

```
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ETLDriver
{
    public static void main(String[] args)
            throws IOException, ClassNotFoundException, InterruptedException
    {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(ETLDriver.class);

        job.setMapperClass(ETLMapper.class);

        job.setNumReduceTasks(0);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
```

3．执行ETL

将本地jar包上传到服务器    进行执行 清洗数据  建立目录 修改权限



```
hadoop fs -mkdir -p /chaoji/
hadoop fs -put /opt/module/datas/video /chaoji
hadoop fs -put /opt/module/datas/user /chaoji
hadoop jar etl.jar com.kgc.etl.ETLDriver /chaoji/video /chaoji/video_etl
```

#### 3 准备工作

#### 3.1 创建表

创建表：chaojivideo_ori，chaojivideo_user_ori，

创建表：chaojivideo_orc，chaojivideo_user_orc

chaojivideo_ori：

```
create table chaojivideo_ori(
    videoId string, 
    uploader string, 
    age int, 
    category array<string>, 
    length int, 
    views int, 
    rate float, 
    ratings int, 
    comments int,
    relatedId array<string>)
row format delimited 
fields terminated by "\t"
collection items terminated by "&"
stored as textfile;
```

chaojivideo_user_ori：

```
create table chaojivideo_user_ori(
    uploader string,
    videos int,
    friends int)
row format delimited 
fields terminated by "\t" 
stored as textfile;
```





chaojivideo_orc：

```
create table chaojivideo_orc(
    videoId string, 
    uploader string, 
    age int, 
    category array<string>, 
    length int, 
    views int, 
    rate float, 
    ratings int, 
    comments int,
    relatedId array<string>)
row format delimited fields terminated by "\t" 
collection items terminated by "&" 
stored as orc;
```

chaojivideo_user_orc：

```
create table chaojivideo_user_orc(
    uploader string,
    videos int,
    friends int)
row format delimited 
fields terminated by "\t" 
stored as orc;
```



#### 3.2 导入ETL后的数据

chaojivideo_ori：

```
load data inpath "/chaoji/video_etl" into table chaojivideo_ori;
```

chaojivideo_user_ori：

```
load data inpath "/chaoji/user" into table chaojivideo_user_ori;
```

####  3.3 向ORC表插入数据

chaojivideo_orc：

```
insert into table chaojivideo_orc select * from chaojivideo_ori;
```



chaojivideo_user_orc：

```
insert into table chaojivideo_user_orc select * from chaojivideo_user_ori;
```



### 4 业务分析

#### 4.1 统计视频观看数Top10

思路：使用order by按照views字段做一个全局排序即可，同时我们设置只显示前10条。

最终代码：

```
select 
    videoId, 
    uploader, 
    age, 
    category, 
    length, 
    views, 
    rate, 
    ratings, 
    comments 
from 
    chaojivideo_orc 
order by 
    views 
desc limit 
    10;
```

#### 4.2 统计视频类别热度Top10

思路：

1) 即统计每个类别有多少个视频，显示出包含视频最多的前10个类别。

2) 我们需要按照类别group by聚合，然后count组内的videoId个数即可。

3) 因为当前表结构为：一个视频对应一个或多个类别。所以如果要group by类别，需要先将类别进行列转行(展开)，然后再进行count即可。

4) 最后按照热度排序，显示前10条。

最终代码：

```
select 
    category_name as category, 
    count(t1.videoId) as hot 
from (
    select 
        videoId,
        category_name 
    from 
        chaojivideo_orc lateral view explode(category) t_catetory as category_name) t1 
group by 
    t1.category_name 
order by 
    hot 
desc limit 
    10;
```

#### 4.3 统计出视频观看数最高的20个视频的所属类别以及类别包含Top20视频的个数

思路：

1) 先找到观看数最高的20个视频所属条目的所有信息，降序排列

2) 把这20条信息中的category分裂出来(列转行)

3) 最后查询视频分类名称和该分类下有多少个Top20的视频

最终代码：

```
select 
    category_name as category, 
    count(t2.videoId) as hot_with_views 
from (
    select 
        videoId, 
        category_name 
    from (
        select 
            * 
        from 
            chaojivideo_orc 
        order by 
            views 
        desc limit 
            20) t1 lateral view explode(category) t_catetory as category_name) t2 
group by 
    category_name 
order by 
    hot_with_views 
desc;
```

#### 4.4 统计视频观看数Top50所关联视频的所属类别排序

思路：

1）先按views数来找出最高的50个视频。按views降序取前50个

2)使用列转行求出，这50个视频,相关的视频编号[relatedId ]==>videoID

 3)使用 videoID与原表联接，得到每一个videoid 的分类名称category

 4)再使用列转行的方式得到每一个videoid及分类名
  5)统计每一个分类名中videoid的个数排序

t1：观看数前50的视频

```
select 
    * 
from 
    chaojivideo_orc 
order by 
    views 
desc limit 
    50;
```

2) 将找到的50条视频信息的相关视频relatedId列转行，记为临时表t2

t2：将相关视频的id进行列转行操作

```
select 
    explode(relatedId) as videoId 
from 
	t1;
```

3) 将相关视频的id和chaojivideo_orc表进行inner join操作

t5：得到两列数据，一列是category，一列是之前查询出来的相关视频id

```
(select 
    distinct(t2.videoId), 
    t3.category 
from 
    t2
inner join 
    chaojivideo_orc t3 on t2.videoId = t3.videoId) t4 lateral view explode(category) t_catetory as category_name;
```

4) 按照视频类别进行分组，统计每组视频个数，然后排行

如果出现错误

Diagnostic Messages for this Task:
Error: Java heap space

查看yran日志 http://node3:8042/logs

从hive报错看是由于物理内存达到限制，导致container被kill掉报错。 
 从报错时刻看是执行reduce阶段报错；故可能reduce处理阶段container的内存不够导致

```
hive (default)> SET mapreduce.map.memory.mb;
mapreduce.map.memory.mb=4096
hive (default)> SET mapreduce.reduce.memory.mb;
mapreduce.reduce.memory.mb=4096
hive (default)> SET yarn.nodemanager.vmem-pmem-ratio;
yarn.nodemanager.vmem-pmem-ratio=4.2

set io.sort.mb=10; 
```

因此，单个map和reduce分配物理内存4G；虚拟内存限制4*4.2=16.8G；

单个reduce处理数据量超过内存4G的限制导致；设置 `mapreduce.reduce.memory.mb=8192` 解决；

最终代码：

![1596770225318](G:\beidaqingniao\bigdata\Hive\hive_07_影音统计\1596770225318.png)

```
select 
    category_name as category, 
    count(t5.videoId) as hot 
from (
    select 
        videoId, 
        category_name 
    from (
        select 
            distinct(t2.videoId), 
            t3.category 
        from (
            select 
                explode(relatedId) as videoId 
            from (
                select 
                    * 
                from 
                    chaojivideo_orc 
                order by 
                    views 
                desc limit 
                    50) t1) t2 
        inner join 
            chaojivideo_orc t3 on t2.videoId = t3.videoId) t4 lateral view explode(category) t_catetory as category_name) t5
group by 
    category_name 
order by 
    hot 
desc;
```

#### 4.5 统计每个类别中的视频热度Top10，以Music为例

思路：

1) 要想统计Music类别中的视频热度Top10，需要先找到Music类别，那么就需要将category展开，所以可以创建一张表用于存放categoryId展开的数据。

2) 向category展开的表中插入数据。

3) 统计对应类别（Music）中的视频热度。

最终代码：

创建表类别表：

![1596770432947](G:\beidaqingniao\bigdata\Hive\hive_07_影音统计\1596770432947.png)

```
create table chaojivideo_category(
    videoId string, 
    uploader string, 
    age int, 
    categoryId string, 
    length int, 
    views int, 
    rate float, 
    ratings int, 
    comments int, 
    relatedId array<string>)
row format delimited 
fields terminated by "\t" 
collection items terminated by "&" 
stored as orc;
```

向类别表中插入数据：

```
insert into table chaojivideo_category  
    select 
        videoId,
        uploader,
        age,
        categoryId,
        length,
        views,
        rate,
        ratings,
        comments,
        relatedId 
    from 
        chaojivideo_orc lateral view explode(category) catetory as categoryId;
```

统计Music类别的Top10（也可以统计其他）

```
select 
    videoId, 
    views
from 
    chaojivideo_category 
where 
    categoryId = "Music" 
order by 
    views 
desc limit
    10;
```

#### 4.6 统计每个类别中视频流量Top10，以Music为例

思路：

1) 创建视频类别展开表（categoryId列转行后的表）

2) 按照ratings排序即可

最终代码：

```
select 
    videoId,
    views,
    ratings 
from 
    chaojivideo_category 
where 
    categoryId = "Music" 
order by 
    ratings 
desc limit 
    10;
```

#### 4.7 统计上传视频最多的用户Top10以及他们上传的观看次数在前20的视频

思路：

1) 先找到上传视频最多的10个用户的用户信息

```
select 
    * 
from 
    chaojivideo_user_orc 
order by 
    videos 
desc limit 
    10;
```

2) 通过uploader字段与chaojivideo_orc表进行join，得到的信息按照views观看次数进行排序即可。

最终代码：

![1596771517159](D:\北大青鸟\大数据\hadoop\HBase\class_01\1596771517159.png)

```
select 
    t2.videoId, 
    t2.views,
    t2.ratings,
    t1.videos,
    t1.friends 
from (
    select 
        * 
    from 
        chaojivideo_user_orc 
    order by 
        videos desc 
    limit 
        10) t1 
join 
    chaojivideo_orc t2
on 
    t1.uploader = t2.uploader 
order by 
    views desc 
limit 
    20;
```

#### 4.8 统计每个类别视频观看数Top10

思路：

1) 先得到categoryId展开的表数据

2) 子查询按照categoryId进行分区，然后分区内排序，并生成递增数字，该递增数字这一列起名为rank列

3) 通过子查询产生的临时表，查询rank值小于等于10的数据行即可。

最终代码：

```
select 
    t1.* 
from (
    select 
        videoId,
        categoryId,
        views,
        row_number() over(partition by categoryId order by views desc) rank from chaojivideo_category) t1 
where 
    rank <= 10;
```

