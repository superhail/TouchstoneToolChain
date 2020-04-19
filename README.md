# TouchstoneToolChain

## 数据库信息采集工具（TouchstoneToolChain）

该程序使用java编写，负责从指定的tidb数据库和sql中抽取touchstone配置文件，用于touchstone的负载生成。

## 配置文件

工具配置文件采用json格式，程序启动时只需要加载配置文件即可，下面是一份配置文件示例。

+ 数据库信息。按照配置文件顺序主要需要配置的字段有，待采集数据库的ip，数据库name，数据库端口，密码和用户名。

+ 数据库版本，无需配置。

+ 文件夹路径

  1. 需要分析查询计划的sql文件夹路径，文件夹内待分析的文件必须以sql结尾，程序会尝试分析每个sql文件内的所有sql语句，单个sql文件内允许包含多组语句，但必须是select语句，其他类型语句未测试。
  2. 结果输出文件夹路径，程序会向指定路径输出touchstone的配置文件和模版化的sql。

+ 类型转化配置

  由于数据库中可能会有很多的属性，但是touchstone处理时，会按照常用类型处理，这里定义了可处理的5种类型，如果数据库中出现了未定义的类型，在配置文件中增加即可，比如bigint，smallint等等。

+ stats http端口 

  用于从http端口采集tidb的schema统计信息

+ select参数配置

  由于在查询计划中，tidb会把condition条件做映射转换，而touchstone使用常见的参数作为输入，因此这个配置文件中定义了如何找到对应的select参数，一般不需要更改

```json
{
    "databaseIp": "biui.me",
    "databaseName": "tpch",
    "databasePort": "4000",
    "databasePwd": "",
    "databaseUser": "root",
    "databaseVersion": "tidb",
    "sqlsDirectory": "conf/sqls",
    "resultDirectory": "touchstoneconf",
    "typeConvert": {
        "INTEGER": [
            "int"
        ],
        "DATETIME": [
            "date"
        ],
        "DECIMAL": [
            "decimal"
        ],
        "VARCHAR": [
            "varchar",
            "char"
        ],
        "BOOL": [
            "bool"
        ]
    },
    "tidbHttpPort": "10080",
    "tidbSelectArgs": {
        "LT": "<",
        "GT": ">",
        "LE": "<=",
        "GE": ">=",
        "EQ": "=",
        "NE": "<>",
        "LIKE": "like",
        "IN": "in"
    }
}
```

## 输出文件

输出包含3部分配置文件

1. schema.conf

   该配置文件包含touchstone需要的schema结构信息和数据分布的统计信息，但需要注意两点

   1. 工具只会输出待分析的query涉及到的table信息，没有涉及到的table不会出现配置文件中
   2. 工具会尝试用query中的信息推测主外键信息，当出现环形依赖或者部分主键依赖，工具无法分析，由于touchstone暂不支持这种schema结构

2. .conf

   该配置文件包含touchstone需要的约束链统计信息，使用`## 文件名_序号`来辨识分析结果中的sql语句，对于未能成功分析的sql，会保留`## 文件名_序号`的标识，而不会输出配置信息，方便后续的手动排查

3. sqls/filename_index.sql

   程序会在指定目录下创建sqls文件夹，输出模版化的查询语句，用于方便的从touchstone中提取的实例化参数填充到sql 中，示例如下。sql参数化模版

   ```sql
   select c_custkey, c_name
   	, sum(l_extendedprice * (1 - l_discount)) as revenue
   	, c_acctbal, n_name, c_address, c_phone, c_comment
   from customer, orders, lineitem, nation
   where c_custkey = o_custkey
   	and l_orderkey = o_orderkey
   	and o_orderdate >= '#25,0,1#'
   	and o_orderdate < '#25,1,1#'
   	and l_returnflag = '#26,0,0#'
   	and c_nationkey = n_nationkey
   group by c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment
   order by revenue desc
   limit 20;
   ```

   Touchstone实例参数

   ```
   	Parameter [id=25, values=[7.35775024210167E11, 7.436040756351471E11], cardinality=300154, deviation=0], 
   	Parameter [id=26, values=[i], cardinality=113037, deviation=0]]
   ```

   同时我们提供了自动填充程序sqlProduction.jar，使用如下命令即可自动填充，请注意只需要使用文件名即可，该程序会自动填充后缀名.sql。

   ```
   java -jar sqlProduction.jar result filename_index 
   ```

   但是sql中可能会遇到无法模版化的情况，此时该工具会将模版信息以注释的方式写在sql头部，请手动模版化。工具运行过程中会产生提示，请按照提示排查。

   ```
   11.sql_0	获取成功
   请注意11.sql_0中有参数出现多次，无法智能替换，请查看该sql输出，手动替换
   ```

   示例如下，由于11.sql中存在两个n_name，而在查询计划中tidb统一了这两个，因此分析时无法智能替换，手动替换这两个即可。

   ```sql
   -- conflictArgs:n_name =:[#2,0,0#],
   select ps_partkey, sum(ps_supplycost * ps_availqty) as value
   from partsupp, supplier, nation
   where ps_suppkey = s_suppkey
   	and s_nationkey = n_nationkey
   	and n_name = 'MOZAMBIQUE'
   group by ps_partkey
   having sum(ps_supplycost * ps_availqty) > (
   	select sum(ps_supplycost * ps_availqty) * 0.0001000000
   	from partsupp, supplier, nation
   	where ps_suppkey = s_suppkey
   		and s_nationkey = n_nationkey
   		and n_name = 'MOZAMBIQUE'
   )
   order by value desc;
   ```

   