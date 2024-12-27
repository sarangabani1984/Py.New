#What is the difference between Spark Core prog and Spark SQL prog?
#Spark core prog the functional transformation on the partitioned collection of opaque memory objects (RDD)
#trans/action on RDD
#Spark sql prog the declarative transformation on the partitioned collection of tuples/records/rows
#sql on tabular dataset

#Bread & Butter1 program
#Resources to Refer for learning Spark SQL - /home/hduser/install/iz_workouts/handson.pdf, ppt (Spark SQL), spark_sql_EL_BB1.py (priority2), spark_sql_ETL_BB2.py (vvv important bread and butter)
#Documentation Reference:
#https://spark.apache.org/docs/3.1.2/sql-data-sources-json.html
#Code Documentation ref & Library ference?
#pyspark.sql.types
#pyspark.sql.session
#pyspark.sql.functions


#How we are going to learn Spark SQL (EL/ETL/ELT):
#HOW to write a typical spark application
#Writing main method, check for for name==main, pass params to the main method, instantiate spark session object .....

#High level concepts - spark_sql_EL_bb1.py start
#1. how to transform from RDD to dataframes using named list/reflection (Row object)
# (not preferred much (unless inevitable)/least bothered/not much important because we preferably create direct DF rather than RDD) and
#2. how to create DF from different (builtin) sources by inferring the schema automatically or manually defining the schema (very important)
#2.A. Different important options (5) & methodologies(5) to create DF
#2.B. Creating DF from different sources with minimal options
#3. how to store the transformed data to targets(builtin)
#4. Usage of diffent options (not much important)
#spark_sql_EL_bb1.py end

#spark_sql_ETL_DSL_bb2.py (vvv important) start
#4. how to apply transformations using DSL(DF) and SQL(view) (main portition level 1)
#5. how to create pipelines using different data processing techniques by connecting with different sources/targets (main portition  level 2)
#above and beyond (Developed/Worked)
#6. how to Standardize/Modernization the code and how create/consume generic/reusable functions & frameworks (level 3)
# - Testing (Unit, Peer Review, SIT/Integration, Regression, User Acceptance Testing), Masking engine,
# Reusable transformation(munge_data, optimize_performance), (Self Served Data enablement) Data movement automation engine (RPA),
# Quality suite/Data Profiling/Audit engine (Reconcilation) (Audit framework), Data/process Observability
#spark_sql_ETL_2.py end

#7. how to the terminologies/architecture/submit jobs/monitor/log analysis/packaging and deployment ...
#8. performance tuning
#After we get some grip on Cloud platform
#9. Deploying spark applications in Cloud
#10. Creating cloud pipelines using spark SQL programs

#HOW to write a typical spark application or spark sql application?
#Ways of Creating (SC + SQLContext + HiveContext) or session?

#when we create spark session object in the name of spark (it comprise of sparkcontext, sqlcontext, hivecontext)
#where we need spark context - to write rdd programs (manually or automatically)
#where we need sql context - to write dsl/sql queries
#where we need hive context - to write hql queries

#Legacy Way (costly and deprecated approach)
from pyspark import SparkContext
sc=SparkContext()#spark context object created, now it a spark application where we can write only core programs using rdd trans/action
from pyspark.sql import SQLContext
sqlContext=SQLContext(sc)#spark context object created, now it a sql application where we can write sql/dsl functions internally it will be rdd trans/actions
from pyspark.sql import HiveContext
hqlContext=HiveContext(sc)
#Though the above way of creating the objects is deprecated, we are understanding this methodology to understand 2 things...
#1. We need the respective contexts to perform respecting operations sparkContext for core, sqlcontext for sql, hivecontext for hive,
#streamContext for streaming, mlcontext for ml, aws is using one context called glueContext(sc) to perform data engineering using a service called AWS Glue...
#2. We got to know what ever the context you use, the background context we have wrap is sparkcontext only

#Or

#Modern Way (simplified and trendy approach)
from pyspark.sql.session import *
spark=SparkSession.builder.getOrCreate()
sc=spark.sparkContext

#spark comprised of sc, sqlContext, hiveContext in future plan to add streamingContext

####1. creating DF from rdd - starts here######

#1. (not much important) how to create (representation) dataframes from RDD using named list/reflection (Row object)
# (not preferred much (unless inevitable)/least bothered/not much important because we preferably create direct DF rather than RDD) and

#How to convert an rdd to dataframe with all possibilities using list option? Not so much important, because we don't create rdd initially to convert to df until it is in evitable
#1. Create an RDD, Iterate on every rdd1 of rdd, split using the delimiter,
#so reading a file from spark context returns RDD and spark session with the function textFile can return rdd rite
rdd1=sc.textFile("file:///home/hduser/sparkdata/nyse.csv")
split_rdd1=rdd1.map(lambda strrow:strrow.split('~'))
print(split_rdd1.collect())
df1=spark.read.csv("file:///home/hduser/sparkdata/nyse.csv")
#2. Iterate on every splitted elements and apply the respective datatype to get the SchemaRDD in a list(tuple) format
schema_rdd=split_rdd1.map(lambda rowcols:(rowcols[0],rowcols[1],float(rowcols[2])))
print(schema_rdd.collect())#list (tuples(str,str,float))

#RDD Transformation
schema_rdd.filter(lambda x:x[0]=='NYSE').map(lambda x:(x[1],x[2])).collect()

#stop working on RDD functions, ASAP represent rdd as dataframe further, to simplify the operation performed on the data
#Below dfs will not have specific column names but datatypes are specified using (schema rdd)
df1=schema_rdd.toDF()#convert rdd to dataframe with default column names _1,_2...

#In the above dataframe we don't have column names, lets define column names also using two different methodologies (1.column list 2. Reflection)
#3. Create column list as per the the structure of data
#methodology 1 - creating df using column list
col_list=["exch","stock","price"]


# 4. Convert the schemaRDD to DF using toDF or createDataFrame apply the column names by calling the column lists
#Below dfs will have specific column  names and specific datatypes also created using the schem_rdd and the column lists defined in above steps
df1=schema_rdd.toDF(col_list) #convert rdd to dataframe with the column list defined above
#or
df1=spark.createDataFrame(schema_rdd,col_list) #convert rdd to dataframe with the column list defined above

#write SQL or DSL queries lavishly , rather than writing RDD transformations/actions on the schema_rdd
#SQL
df1.createOrReplaceTempView("viewname")
df2=spark.sql("select stock,price from viewname where exch='NYSE'")
df2.show()

#DSL (Spark Domain specific language) (Dataframe functions)
df2=df1.where("exch='NYSE'").select("stock","price")
df2.show()

#or - methodology 2 - creating df using reflection concept

#How to convert/represent an rdd as dataframe using Reflection to the Row class?
# Not so much important, because we don't create rdd initially to convert to df until it is in evitable

#1. Create an RDD, Iterate on every rdd1 of rdd, split using the delimiter
rdd1=sc.textFile("file:///home/hduser/sparkdata/nyse.csv")
split_rdd1=rdd1.map(lambda strrow:strrow.split('~'))
print(split_rdd1.collect())
#[['NYSE', 'CLI', '35.3'], ['NYSE', 'CVH', '24.62'], ['NYSE', 'CVL', '30.2']]
#2. Reflect every splitted elements to the Row class and get a Row object based SchemaRDD created with both columnnames and datatype applied
#import the Row object
from pyspark import Row
schema_rdd=split_rdd1.map(lambda rowcols:Row(exchange=rowcols[0],stock=rowcols[1],price=float(rowcols[2])))
#[Row(_c0='NYSE', _c1='CLI', _c2='35.3'), Row(_c0='NYSE', _c1='CVH', _c2='24.62'), Row(_c0='NYSE', _c1='CVL', _c2='30.2')]


#3. We can skip step 3 for creating the columname seperately as list, because we had column names already added in the Row

# 4. Convert the schemaRDD to DF using toDF or createDataFrame using concept of Reflection using Row Objects
df1=schema_rdd.toDF()
#Or
df1=spark.createDataFrame(schema_rdd)

#sql programming (familiar)
df1.createOrReplaceTempView("viewname")
df2=spark.sql("select stock,price from viewname where exch='NYSE'")
df2.show()

#dsl programming
df2=df1.where("exch='NYSE'").select("stock","price")
df2.show()

#Rather writing the code like above (either schemardd+collist or rowrdd to DF) (create rdd then converting to DF),
# preferable write the code (directly create DFs) using the modules as given below

#Word count1 in spark using core programming
'''
hadoop spark hadoop spark kafka datascience
spark hadoop spark datascience
informatica java aws gcp
gcp aws azure spark
gcp pyspark hadoop hadoop
'''
rdd1=sc.textFile("file:///home/hduser/sparkdata/courses.log")
rdd2=rdd1.flatMap(lambda x:x.split(" "))
print(rdd2.countByValue())

#Word count2 in spark using core programming
rdd1=sc.textFile("file:///home/hduser/sparkdata/courses.log")
rdd2=rdd1.flatMap(lambda x:x.split(" ")).map(lambda x:(x,1))
rdd3=rdd2.reduceByKey(lambda x,y:x+y)
print(rdd3.collect())


#Interview question? How to do word count in spark using RDD transformations and actions?
# Quick Usecase - Spark wordcount using core and dsl/sql to prove spark sql is a UNIFIED tool?
#A typical example for we are forced to create rdd then df then temp view query.
#1. create an rdd to convert the data into structured from unstructured
rdd1=sc.textFile("file:///home/hduser/sparkdata/courses.log")
#rdd2=rdd1.flatMap(lambda x:x.split(" ")).map(lambda x:(x,1))#Structured data
rdd2=rdd1.flatMap(lambda x:x.split(" ")).map(lambda x:(x,))#Structured data
#rdd2=rdd1.flatMap(lambda x:x.split(" ")).map(lambda x:(x,1))#Don't write rdd program
#rdd3=rdd2.reduceByKey(lambda x,y:x+y)#Don't write rdd program
#2. represent rdd as dataframe (ASAP)
col_list1=list(["course_name"])
df1=rdd2.toDF(col_list1)

#3. write dsl/sql queries
df1.createOrReplaceTempView("view1")
spark.sql("select course_name,count(1) from view1 group by course_name").show()

#What we learned in this section 1?
#1. How to define spark session using modern methodology or sc, sqlc, hivec seperately using legacy methodology
#2. How to define schema rdd (tuples/Row(tuples)) and applied column names using list or reflection
#3. How to represent schema rdd in a form of dataframe using toDF or createDataFrame functions
#4. Fundamental dsl on dataframe and sql on tempview

####1. creating DF from rdd - ends here######


####2.Starting- creating DF directly from different sources using different builtin functions using different (important) options using different methodologies (very important)######
#We will forget RDD from here, spark will obviously use rdd only internally (architecture part of SQL)
#2. how to create DF from different sources (using different options) by inferring the schema automatically
# or manually defining the schema (very important)

# 3+2 Important - Options to recall
#delimiter/sep='~' (data is having ~ as delimiter)
#header=True (if data contains header)
#inferSchema=True (Asking spark to go and analyse (all the rows) and identify the data type)
#by default options are set with these values - sep=",",header=False,inferSchema=False

#2.A. How to create DF using different spark session need, options, methodologies
# 1.Creation of DFs needed spark session or sqlContext and DF will be immediately evaluated and lazy executed
# 2.Creating DFs directly using different options applied (important inferschema, delimiter, header, schema, mode)
# 3.Creating DFs using multiple methodologies (inline,option,options,format/load,load+format)
###############################################################################################################################################################################
#1. spark session object importance
# we can't create DF only by using spark context?
# spark session (includes sqlContext naturally) or direct sqlcontext in legacy way) ensure we have spark and sqlContext available to create DF
spark=SparkSession.builder.getOrCreate()

#DF will be doing Immediate Evaluation (existance of source data, header, delimiter, columns & datatype) & Lazy Execution
#When i define Dataframe the rdd will be doing immediate evaluation rather than lazy evaluation, but execution(processing) will be lazy only....
#df1=spark.read.csv("file:///home/hduser/empdata12333.csv")#data loaded into memory, evaluated and deleted by GC

#2. We can use read module/method for invoking different builtin functions for applying multiple options
#column names starts with _c0, _c1... (if we don't have header),  with default delimiter ',' with default datatype String for all columns

df1=spark.read.csv("file:///home/hduser/sparkdata/nyse.csv")
df1.show(2)

#sep/delimiter - By default comma is seperator ie used, if we have other comma as a delimiter then use it appropriately
df1=spark.read.csv("file:///home/hduser/sparkdata/nyse.csv",sep="~")
df1.show(2)
df1.printSchema()

#inferSchema- If we use inferSchema=True, then all records will be analysed and identify the datatype of every columns
df1=spark.read.csv("file:///home/hduser/sparkdata/nyse.csv",sep="~",inferSchema=True)
df1.show(2)
df1.printSchema()

#Header- If we use header=True, then first row will be taken as header
df1=spark.read.csv("file:///home/hduser/sparkdata/nyse_header.csv",sep="~",inferSchema=True,header=True)
df1.show(2)
df1.printSchema()

#If I receive header from the source, we have to mention header,true option
df1=spark.read.csv("file:///home/hduser/sparkdata/nyse_header.csv",sep="~",header=True)

#If I don't receive header from the source, but we have to mention header,false option
df1=spark.read.csv("file:///home/hduser/sparkdata/nyse.csv",sep="~")

#At the time of creating DF - If I receive header from the source, but we wanted to change the column name (because I don't want spark to give _c0,_c1...)
df1=spark.read.csv("file:///home/hduser/sparkdata/nyse.csv",sep="~",inferSchema=True,header=False).toDF("exchange","stock","price")
df1.printSchema()

#If I have data in comma delimited format and no header is there, then we can mention only one option
df1=spark.read.csv("file:///home/hduser/sparkdata/nyse.csv",inferSchema=True)

#Options to recall
#3 Important options are - delimiter, inferschema, header

#2 more Important options are - schema, mode

#Option4 - schema option
###How to define schema manually/custom way (very important) starts here###
#Why we have to go with manual/custom way of defining structure/schema for our dataframe?
#1. For customized datatype definition rather than double i want it as float
#2. I don't want spark to take long time for inferring the schema (to overcome we can use datasampling option)
#3. If the structure of the data is fixed, usage of manual schema definition is more standard... to avoid spark to go and infer everytime
#4. We want to define/control both colum name and data type in one place including (not null constraint (not much important))

#StructType - I will hold the list of StructField (like a tablename in hive)
#StructField - I will hold the columname, datatype and nullable true/false (column datatype in hive)
#for eg. create table tbl1(colname datatype) - here tbl1 is like StructType, colname datatype is kept under StructField
#pseudo code - StructType(list([StructField("colname1",StringType(),False),StructField(),StructField()]))
#syntax how to remember StructType([StructField("col",DataType(),True)])

#/usr/local/spark/python/pyspark/sql/types.py

from pyspark.sql.types import StructType,StructField,StringType,FloatType
#customsch=StructType([StructField("colname",StringType(),False),StructField("colname",StringType(),False])
customsch=StructType([StructField("exchange",StringType(),True),StructField("stock",StringType(),True),StructField("price",FloatType(),True)])
df1=spark.read.csv("file:///home/hduser/sparkdata/nyse.csv",sep="~",schema=customsch)

#Nullable true/false constraint (rule for checking not null)
'''
NYSE~CLI~35.3
NYSE~CVH~24.62
~CAN~110.1
NYSE~CVL~30,02
'''
from pyspark.sql.types import StructType,StructField,StringType,FloatType
customsch=StructType([StructField("exchange",StringType(),False),StructField("stock",StringType(),True),StructField("price",FloatType(),True)])
df1=spark.read.csv("file:///home/hduser/sparkdata/nyse_corrupt.csv",sep="~",schema=customsch)
df1.printSchema()
df1.show()
#https://issues.apache.org/jira/browse/SPARK-10848
df2=df1.rdd.toDF(customsch)
df2.printSchema()
df2.show()

###How to define schema manually (very important) ends here###

#Tale of usage of infer schema vs custom schema?
#mode option in spark dataframe
#option5 - mode (permissive(default), dropmalformed, failfast)-
# Used along with the schema defined to control the data while creating DF
#mode (read) (permissive(default), failfast, dropmalformed)
#permissive - permit everything by not evaluate the data with the structure defined hence don't throw any error, unfitted data/columns are considered as null
#failfast - fail immediately by evaluate the data with the structure defined throw error if any data doesn't fit the structure (datatype/number of columns)
#dropmalformed - drop the unwanted data ( if it doesn't fit the structure) by comparing with the structure defined (datatype/number of columns)

#permit with nulls
df1=spark.read.csv("file:///home/hduser/sparkdata/nyse_corrupt.csv",sep="~",schema=customsch,mode="permissive")

#drop malformed data with issues
df1=spark.read.csv("file:///home/hduser/sparkdata/nyse_corrupt.csv",sep="~",schema=customsch,mode="dropmalformed")

#fail if any data issue in any column
df1=spark.read.csv("file:///home/hduser/sparkdata/nyse_corrupt.csv",sep="~",schema=customsch,mode="failfast")

#permit all data with the structure adjusted accordingly as per the data received and i will fix the data issue in my side
df1=spark.read.csv("file:///home/hduser/sparkdata/nyse_corrupt.csv",sep="~",inferSchema=True)

#When can we go with inferschema?
#We use mostly defined schema using structtype and structfield for the defined dataset in production
#We use some time the inferschema when the volume of data is small and data is variable in production
# or for development (if i have large number of columns, still i do manual creation of structtype? no, u can go with inferschema for Development,
# understand the structure and apply in the custom schema)
# bug fixing purpose we may use inferschema (we allow all data through infer schema, then we find the culprit)
df1=spark.read.csv("file:///home/hduser/sparkdata/nyse_corrupt.csv",sep="~",inferSchema=True)
'''df1.show()
+----+---+-----+
| _c0|_c1|  _c2|
+----+---+-----+
|NYSE|CLI| 35.3|
|NYSE|CVH|24.62|
| BSE|CAN|110.1|
|NYSE|CVL|30,02|  Culprit is 30,02 rather than 30.02
|NYSE|AAA| null|  
+----+---+-----+
'''

df1.createOrReplaceTempView("view1")
spark.sql("select _c0,_c1,cast(replace(_c2,',','.') as float) _c2 from view1").printSchema()

#3. Different way/methodologies to define dataframe
#inline argument, option, options, format & load, load with (format) inline arguments
#Five methologies we have to apply options/parameters to provide additional features to the data
'''1. inline methodology 
2. option
3. options
4. format/load
4. load with (format) in line
'''

#a. Inline arguments methodology to define dataframes with lot of built in options to use in a guided way
df1=spark.read.csv(path="file:///home/hduser/sparkdata/nyse.csv",sep="~",inferSchema=True,header=False)
#Conclusion is - use inline argument methodology (for builtin functions) if we want to have multiple different flavors of options in a guided way

#b. Using Option for providing individual minimal options of one or two, it will work for both builtin (csv/orc/json) and external source functions (redshift/kafka..)
df1=spark.read.option("delimiter","~").option("inferschema","True").option("header",False).csv(path="file:///home/hduser/sparkdata/nyse.csv")

#c. Using Option for providing individual multiple options, it will work for both builtin (csv/orc/json) and external source functions (redshift/kafka..)
df1=spark.read.options(delimiter="~",inferschema="TRUE",header=False).csv(path="file:///home/hduser/sparkdata/nyse.csv")

#D. Format & load methodology to define dataframes from mainly external sources (it works for built sources also)
#Eg:
#spark.read.format("io.github.spark_redshift_community.spark.redshift").option("url","jdbc:redshift://redshift-cluster-wd28.cn3haetn4qhf.us-east-1.redshift.amazonaws.com:5439/dev?user=inceptezuser&password=Inceptezuser123")\
# .option("forward_spark_s3_credentials", True)\
# .option("dbtable", "public.patients").option("tempdir", "s3a://com.inceptez.shellbucket/redshifttempdata/").load()
df1=spark.read.format("csv").option("header",False).option("inferschema",True).load(path="file:///home/hduser/sparkdata/nyse.csv",sep="~")

#Conclusion is - use format and load methodology is used if we want to source the data from any sources mainly EXTERNAL (internal or external)

#E. load methodology to define dataframes adding format as an argument - from mainly external sources (it works for built sources also)
df1=spark.read.option("header",False).option("inferschema",True).load(path="file:///home/hduser/sparkdata/nyse.csv",format="csv",delimiter="~")

#Important Conclusion is - By default if we don't mention the format, parquet format will be considered as default preferred data format for spark

#Summary in 2.A: We learned upto defining dataframe from rdds using reflection, list options,
# creating dataframes using different methodologies and options (important 5 options)
#compared inferschema with customschema

#2.A. Ending -
# Options - delimiter/sep, header, inferSchema,schema ( Custom schema), Mode ( persmissive, dropmalformed, failfast)
# Methodologies - option(opt1,value1), options(opt1=value,opt2=value), csv(filename,opt1=value,opt2=value),
# format(csv).option/options.load("file")

#Before learning 2.B, try to learn 3 and then come back to 2.B
#2.B. Starting - Create dataframes using the modules (builtin (csv, orc, parquet, jdbc,table, json) / external later (cloud, api, nosql))
# or programatically by applying DSL/SQL we are going to learn in BB2

#CSV Module - From batch, bulk regular standard dataset
df_csv1=spark.read.csv("hdfs:///user/hduser/df1data",header=True,sep='|',inferSchema=True)
from pyspark.sql.types import StructType,StructField,FloatType,StringType
customsch=StructType([StructField("exch",StringType(),True),StructField("stock",StringType(),True),StructField("price",FloatType(),True)])
df_csv1=spark.read.csv("file:///home/hduser/sparkdata/nyse.csv",header=False,sep='~',schema=customsch,mode="permissive",enforceSchema=False)

#JSON Module - Semistructured data (json is used for all the below 4 reasons with large volume also)
#when it is REST calls, json is used..At the same time in data dump or snapshot scenarios CSV is the preferred one in organizations
#When do we go for JSON? If we get data from some applications (IOT (Device), API, Webservises, Social media etc.,) or If the data is variable in nature (column order/number of columns are evolving)
#We don't go with Json for handling large volume of data, but if the API systems/WS systems are sending large volume of data it can be sent in json format
#1. We can have variable number of fields in every rows (added/removed) of data with different orders (as given above)
#2. Data is stored in a hirarchichal(nested) + complex fashion to map any number of elements or data sets. eg. capture customer, product, location, transaction ..
#in a single object (record)
#3. Avoid duplication of data (unnecessarily occupy space) semistruct: wrapped (3d data)
#4. Performance wise semi-structure will be better because it uses intelligent storage and network transfer of data (because it applies better serialization/deserialization by the system itself)

df_json1=spark.read.json("file:///home/hduser/sparkdata/nysejson")#no header/sep/inferSchema option (all are inbuilt for json)
df_json1.show()

customsch=StructType([StructField("exch",StringType(),True),StructField("stock",StringType(),True),StructField("price",FloatType(),True),StructField("closingprice",FloatType(),True)])
df_json1=spark.read.json("file:///home/hduser/sparkdata/nysejson",schema=customsch)
df_json1.show()


#Serialized builtin data format (Orc/Parquet)
#Parquet Module (Spark preferred format)- Serialized data (optimized)
#Characteristics: Columnar data, pushdown optimization, pruning, intelligent data format, intelligent storage size, featuristic data format (header/type/format is preserved)
#Application of Parquet - intermediate data, raw (schema migrated) data, hive table data, optimized data, Archival, Deltalake (Databricks) Parquet
#customsch=StructType([StructField("exch",StringType(),True),StructField("stock",StringType(),True),StructField("price",FloatType(),True),StructField("closingprice",FloatType(),True)])
df_par1=spark.read.parquet("file:///home/hduser/sparkdatadefault")#custom schema option is not supported since orc is self contained/intelligent data format

#Orc Module (Hive preferred but spark prefers orc if parquet is not used)- Serialized data (optimized)
#Characteristics: Columnar data, pushdown optimization, pruning, intelligent data format, featuristic data format (header/type/format is preserved), interiem data, background of deltalake(databricks) is parquet not orc
#Application of orc - intermediate data, raw (schema migrated) data, hive table data, optimized data, Archival
#customsch=StructType([StructField("exch",StringType(),True),StructField("stock",StringType(),True),StructField("price",FloatType(),True),StructField("closingprice",FloatType(),True)])
df_orc1=spark.read.orc("file:///home/hduser/orcdata")#custom schema option is not supported since orc is self contained/intelligent data format
from pyspark.sql.functions import *
df_orc2=df_orc1.withColumn("closingprice",col("closingprice").cast("double"))
df_orc2.printSchema()

#Avro Module (Not a built in function in Spark, we have to explicitly import avro libraries and use - later)

#Hive Module (We want to perform DSL or HQL on hive tables using spark)
df_hive1=spark.read.table("default.nyse_json_tbl_part_buck1")
#DSL (only works in spark)
df_hive1.where("datadt='2024-05-25' and exch='NYSE'").select("stock","price").show()#Highly performant DSL query

#SQL (works everywhere hive/spark/bq/redshift/mysql/sqlserver/athena/synapse....)
df_hive1.createOrReplaceTempView("df_hive_view1")
spark.sql("select stock,price from df_hive_view1 where datadt='2024-05-25' and exch='NYSE'").show()#Highly performant SQL query
spark.sql("select stock,price from df_hive_view1 where datadt='2024-05-25' and exch='NYSE'").explain()

#Database Source (jdbc) Module (used to read data from database sources present in onprem or cloud)
#Spark JDBC (feature for data extraction/ingestion/exchange from DB source)=Sqoop is tool for data extraction/ingestion/exchange from DB source

df_payments=spark.read.jdbc(url="jdbc:mysql://34.16.71.60/custpayments",table="payments",
                            properties={"user":"irfan","password":"Inceptez@123","driver":"com.mysql.cj.jdbc.Driver"})
df_payments.show()
df_payments.write.csv("/user/hduser/dbcsv",header=True,sep='|',mode="overwrite")
df_payments.write.parquet("/user/hduser/dbparquet",mode="overwrite",compression='gzip') #preferred way

#If Spark is there then why we need Sqoop or vice versa?
#Sqoop (EL Tool) is meant for data ingestion but spark (ETL Tool) has option to do data ingestion from DB
#Sqoop has more features specific to data ingestion - Spark has limited features but it support federation and unification
#Sqoop will be preferrably used for data migration, consolidation and convergence from different structured DB sources

#sqoop import --connect --table --username --password --delete-target-dir --target-dir -m 1 --check-column --incremental-append --last-value --fetch-size

#databases in cloud - empoffice, ordersproducts, custpayments

#Interview Question: Is it possible to transfer data between 2 different spark applications?
# Spark has a big challenge of tranferring data between applications

#Interview Question: I have executor created with 1 GB capacity, but source DB(singapore) has 2gb data,
# I want to cache the entire data into spark (US) ?
#I have a spark resources available, which is lesser than the data that I am going to process, how to improve the performance still retaining the same resource capacity?
#To achieve better performance, 1. Convert the data into intelligent format (Optimized Row/Column), 2. Partition the data, 3. Cluster(bucket) the data
#By converting to intelligent format of orc/parquet - we are getting PDO, Predicate/projection, pruning, serialization, compression features 4. use persist with memory_and_disk
df_payments=spark.read.jdbc(url="jdbc:mysql://34.16.71.60/custpayments",table="payments",
                            properties={"user":"irfan","password":"Inceptez@123","driver":"com.mysql.cj.jdbc.Driver"})

from pyspark.storagelevel import StorageLevel
df_payments.persist(StorageLevel.MEMORY_AND_DISK)#All 2 gb data from DB will be pulled once for all and will be stored in memory as 1gb and disk as 1 more gb
df_payments.write.csv("/user/hduser/dbcsv",header=True,sep='|',mode="overwrite")#First time DB hit will happen and data will be cached
df_payments.write.parquet("/user/hduser/dbparquet",mode="overwrite",compression='gzip') #DB hit will not happen, data will be read from cached memory and disk

#2.B. Ending - Create dataframes using the modules (builtin (csv, orc, parquet, jdbc,table, json)

#3. How to store the DFs raw/transformed data into different targets in different formats (builtin)
#I have a dataframe created by some way, how can store into different targets using different builtin functions (with very minimal options used)
#syntax to write DF into some storage
#df.write.csv("///")

#Rafeeq - On topof temp view can we create another temp view ? I tried but it failed with some error
df_json1.createOrReplaceTempView("dfjsonview1")
df_json1.select("exch","price").createOrReplaceTempView("dfjsonview2")
#or
spark.sql("select exch,price from dfjsonview1").createOrReplaceTempView("dfjsonview2")
#spark.sql("insert into dfjsonview2 select exch,price from dfjsonview1") #this is not possible
#pyspark.sql.utils.AnalysisException: Inserting into an RDD-based table is not allowed.;

#Writing in a csv format into hdfs
df1=spark.read.csv("file:///home/hduser/sparkdata/nyse.csv",sep="~",inferSchema=True).toDF("exch","stock","price")
df1.write.csv("hdfs:///user/hduser/df1data")

#writing DF to CSV into HDFS using important options
df1.write.csv("hdfs:///user/hduser/df1data",header=True,mode="append",sep="|")

#Writing in a json format into hdfs
#story -> i receive csv data in linux file system, i did schema migration to write in json format into our datalake
df1.write.json("hdfs:///user/hduser/jsondata",mode="overwrite")

#Writing in a orc format into hdfs
df1.write.orc("hdfs:///user/hduser/orcdata",mode="overwrite")
df_json1.write.save("file:///home/hduser/orcdata",format="orc",mode="overwrite")#using write.save (overriding with orc)

#Writing in a parquet format
df1.write.parquet("hdfs:///user/hduser/parquetdata",mode="overwrite")
df_json1.write.save("file:///home/hduser/sparkdatadefault",mode="overwrite")#using write.save (default parquet is loaded)
df_json1.write.parquet("file:///home/hduser/sparkdatadefault")

#Writing in a Hive Table
#Using DSL
df_json1.write.saveAsTable("default.nyse_json_tbl",mode="append")
df_json1.write.insertInto("default.nyse_json_tbl",overwrite=False)#not preferred to use since it creates insert statements which is slow

df_json2=df_json1.withColumn("datadt",current_date()).drop("closingprice")
#Spark can help create bucketed table in hive using spark native algorithm called MurMur
#Below table is highly performant because partitioned, bucketed, parquet and compressed with snappy
df_json2.write.partitionBy("datadt").bucketBy(numBuckets=3,col="stock").saveAsTable("default.nyse_json_tbl_part_buck1",mode="append")

#Conclusion: To write Data into hive table, spark will use his own algo only for bucketing, rest of all operation hive algorithm is supported by spark

#Using (unmodified with few limitations) HQL we can write/read data from hive table also
#Using SQL & HQL
df_json1.createOrReplaceTempView("dfjsonview1")
spark.sql("create table default.nyse_json_tbl1 row format delimited fields terminated by ',' as select * from dfjsonview1")
spark.sql("create external table default.nyse_json_tbl_ext row format delimited fields terminated by ',' location '/user/hduser/nyse_json_tbl_ext' as select * from dfjsonview1")

spark.sql("insert overwrite table default.nyse_json_tbl1 select upper(exch),stock,price+1,coalesce(closingprice,0) from dfjsonview1")

spark.sql("create table default.nyse_json_tbl_part(exch string,stock string,price float) partitioned by (datadt date) row format delimited fields terminated by ',' ")
spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
spark.sql("insert overwrite table default.nyse_json_tbl_part select upper(exch),stock,price,current_date() from dfjsonview1")

#Interview Question: Can we store data into hive bucketed tables using spark program? No
#Limitation of Spark+hive - Hive bucketed tables (uses Hash bucketing algorithm) cannot be loaded using spark program because spark uses a bucketing algorithm call MurMur
spark.sql("create table default.nyse_json_tbl_part_buck(exch string,stock string,price float) partitioned by (datadt date) clustered by (stock) into 3 buckets row format delimited fields terminated by ',' ")
spark.sql("insert overwrite table default.nyse_json_tbl_part_buck select upper(exch),stock,price,current_date() from dfjsonview1")
#pyspark.sql.utils.AnalysisException: Output Hive table `default`.`nyse_json_tbl_part_buck` is bucketed but Spark currently does NOT populate bucketed output which is compatible with Hive.


#Writing in a cloud DB from lfs our on prem cluster
df1.write.jdbc(url="jdbc:mysql://34.16.71.60:3306/sampledb",table="nyse_stock",mode="overwrite",properties={"user":"irfan","password":"Inceptez@123"})

#Writing in a cloud DB from lfs our on prem cluster

#3. How to store the DFs raw/transformed data into different targets in different formats (builtin)
#I have a dataframe created by some way, how can store into different targets using different builtin functions (with very minimal options used)
#syntax to write DF into some storage
#df.write.csv("///")

#Writing in a csv format into hdfs
df1=spark.read.csv("file:///home/hduser/sparkdata/nyse.csv",sep="~",inferSchema=True).toDF("exch","stock","price")
df1.write.csv("hdfs:///user/hduser/df1data")

#writing DF to CSV into HDFS using important options
df1.write.csv("hdfs:///user/hduser/df1data",header=True,mode="append",sep="|")

#Writing in a json format into hdfs
#story -> i receive csv data in linux file system, i did schema migration to write in json format into our datalake
df1.write.json("hdfs:///user/hduser/jsondata",mode="overwrite")


#Writing in a orc format into hdfs
df1.write.orc("hdfs:///user/hduser/orcdata",mode="overwrite")

#Writing in a parquet format into hdfs
df1.write.parquet("hdfs:///user/hduser/parquetdata",mode="overwrite")

#Writing in a cloud DB from lfs our on prem cluster
df1.write.jdbc(url="jdbc:mysql://34.16.71.60:3306/sampledb",table="nyse_stock",mode="overwrite",properties={"user":"irfan","password":"Inceptez@123","driver":"com.mysql.cj.jdbc.Driver"})

#Writing in a cloud DB from lfs our on prem cluster
df_payments=spark.read.jdbc(url="jdbc:mysql://34.16.71.60/custpayments",table="payments",properties={"user":"irfan","password":"Inceptez@123"})
df_payments.show()
#databases in cloud - empoffice, ordersproducts, custpayments

#Interview Question - I want to write the output files into one or more, how to control the number of output files?
df1.coalesce(1).write.csv("hdfs:///user/hduser/df1data",header=True,mode="append",sep="|")

#4. Revisiting all the function with additional options... (Not important, but good to know once for all)
#csv,json,orc,parquet,jdbc,hive while writing and reading
#****Possible way of calling the csv and all other functions with different other options refering the document
#All possible methodologies
#Dont spend much time on these options for learning (later in realtime work for implementing, think of using these optionss)
#https://spark.apache.org/docs/3.2.1/sql-data-sources-csv.html
#https://spark.apache.org/docs/3.2.1/sql-data-sources-json.html
#https://spark.apache.org/docs/3.2.1/sql-data-sources-jdbc.html

#https://issues.apache.org/jira/browse/SPARK-19256

#****Possible way of calling the csv and all other functions with different other options refering the document

#JDBC Additional Options:
#JDBC Read options
#Performance optimization while using JDBC?
#Sqoop -> pushdown optimization, fetch size, number of mapper, boundary query ...
#Sqoop  - Spark
#-m     - numPartitions
#--split-by - partitionColumn
#fetchsize - fetchsize
#batchsize - batchsize

#Pushdown optimization
#Let db engine is used to calculate boundary
#Database do all heavy work of finding min/max running the query in your engine and give only min/max value to spark
#How to write free form query to pull the data using spark rather than pulling the entire table data?
#boundary_query="(select min(customerNumber) min_cid,max(customerNumber) max_cid from payments)temp"
df_payments_boundary=spark.read.jdbc(url="jdbc:mysql://34.16.71.60/custpayments",table="(select min(customerNumber) min_cid,max(customerNumber) max_cid from payments)temp",
                            properties={"user":"irfan","password":"Inceptez@123"})

#or
#Let spark engine is used to calculate boundary
#Database don't do any heavy work just give all data to spark, let Spark do all heavy work of finding min/max running the query on entire table keeping in spark memory
#df_payments.createOrReplaceTempView("view1")
#spark.sql("select min(customerNumber),max(customerNumber) from view1").show() #pushdown applied or not? No

df_payments=spark.read.jdbc(url="jdbc:mysql://34.16.71.60/custpayments",table="payments",
                            properties={"user":"irfan","password":"Inceptez@123","partitionColumn":"customerNumber","numPartitions":"2",
                            "lowerBound":"1000","upperBound":"10000"})#partition skewing occurs

print(df_payments.rdd.glom().collect())

df_payments=spark.read.jdbc(url="jdbc:mysql://34.16.71.60/custpayments",table="payments",
                            properties={"user":"irfan","password":"Inceptez@123","partitionColumn":"customerNumber","numPartitions":"2",
                            "lowerBound":"103","upperBound":"496","fetchSize":"1000","queryTimeout":"60","isolationLevel":"READ_COMMITTED","pushDownPredicate":"true","pushDownAggregate":"true"})
#partition skewing will not occur provided the partitioncolumn is high in cardinality

for i in df_payments.rdd.glom().collect():
    print(len(i))
#159
#114

#JDBC Write options
df_payments.write.jdbc(url="jdbc:mysql://127.0.0.1/custpayments",table="cloud_db_payments",mode="overwrite",
                                        properties={"user":"root","password":"Root123$","driver":"com.mysql.cj.jdbc.Driver",
                                                    "batchsize":"10","numPartitions":"4","queryTimeout":"60","truncate":"true"})