#PySpark SQL Programming is Platform agnostic (works only with spark not anywhere else)#
#Projects evolution:
#hive project -> Cloudera - spark project -> a&b things (frameworks reusable, masking, quality, automation) ->
# GCP(DP)/Azure(DB) cloud migration(lift & shift) -> cloud industrialization/modernization/standardization (GCP - df,dp,gcs,bq,cc/af), (ADF,ADL,ADB,Synapse,workflow)

#spark_sql_ETL_DSL_bb2.py start (Very very very Important Bread & Butter2) - FBP-SQL/DSL (FBP-SQL)
#How to use Python to control the SQL (PySpark SQL (Python (Minimum)-> Pyspark(DSL) (complete) -> SQL(Complete)))
#5. how to apply transformations/business logics/functionality/conversion using DSL(DF) and SQL(view) (main portion level 1)
#hive, spark(DSL/SQL) - cloud native components (SQL)
#6. how to create pipelines using different data processing techniques by connecting with different sources/targets (main portion  level 2)
#above and beyond (Developed/Worked)

#7. how to Standardize/Modernization the code and how create/consume generic/reusable functions & frameworks (level 3)
# - Testing (Unit, Peer Review, SIT/Integration, Regression, User Acceptance Testing), Masking engine,
# Reusable transformation(munge_data, optimize_performance), (Self Served Data enablement) Data movement automation engine (RPA),
# Quality suite/Data Profiling/Audit engine (Reconcilation) (Audit framework), Data/process Observability
#spark_sql_ETL_2.py end

#8. how to the terminologies/architecture/packaging and deployment/submit jobs/monitor/log analysis ...
#9. performance tuning - learn in our interview discussion
#After we get some grip on Cloud platform
#10. Deploying spark applications in Cloud
#11. Creating cloud pipelines using spark SQL programs

##### LIFE CYCLE OF ETL and Data Engineering PIPELINEs
# VERY VERY IMPORTANT PROGRAM IN TERMS OF EXPLAINING/SOLVING PROBLEMS GIVEN IN INTERVIEW ,
# WITH THIS ONE PROGRAM YOU CAN COVER ALMOST ALL DATAENGINEERING FEATURES
#Tell me about the common transformations you performed, tell me your daily roles in DE, tell me some business logics you have writtened
#How do you write an entire spark application, levels of DE pipelines or have you created DE pipelines what are the transformations applied,
#how many you have created or are you using existing framework or you created some framework?

#5. how to apply transformations/business logics/functionality/conversion using DSL(DF) and SQL(view) (main portion level 1)
#hive, spark(DSL/SQL) - cloud native components (SQL)
#6. how to create pipelines using different data processing techniques by connecting with different sources/targets (main portion  level 2)
#above and beyond (Developed/Worked)

#Source -> actual/sample data -> 1. Classification/Onboarding (DGovernance/Data Stewards/DSecurity/Data Solution Architect (Collibra, ICP4D) - filter/redact/mask/classify & cluster/tagging/lineage/...)
#2. DEngineers -> Raw Data -> apply data munging process -> other strategies...

'''
TRANSFORMATION
Starting point - (Data Governance (security) - Tagging, categorization, classification, masking/filteration)
1. Data Munging - Process of transforming and mapping data from Raw form into Tidy(neat/usable) format with the
intent of making it more appropriate and valuable for a variety of downstream purposes such for further Transformation/Enrichment, Egress/Outbound, Analytics, Model application & Reporting
a. Passive - Data Discovery (EDA) (every layers ingestion/transformation/consumption) - (Data Governance (security) - Tagging, categorization, classification) (EDA) (Data Exploration) - Performing an (Data Exploration) exploratory data analysis of the raw data to identify the attributes and patterns.
b. Active - Combining Data + Schema Evolution/Merging/Melting (Structuring)
c. Validation, Cleansing, Scrubbing - Identifying and filling gaps & Cleaning data to remove outliers and inaccuracies
Preprocessing, Preparation
Cleansing (removal of unwanted datasets eg. na.drop),
Scrubbing (convert of raw to tidy na.fill or na.replace),
d. Standardization, De Duplication and Replacement & Deletion of Data to make it in a usable format (Dataengineers/consumers)

2. Data Enrichment - Makes your data rich and detailed
a. Add, Remove, Rename, Modify/replace
b. split, merge/Concat
c. Type Casting, format & Schema Migration

3. Data Customization & Processing - Application of Tailored Business specific Rules
a. User Defined Functions
b. Building of Frameworks & Reusable Functions

4. Data Curation
a. Curation/Transformation
b. Analysis/Analytics & Summarization -> filter, transformation, Grouping, Aggregation/Summarization

5. Data Wrangling - Gathering, Enriching and Transfomation of pre processed data into usable data
a. Lookup/Reference
b. Enrichment
c. Joins
d. Sorting
e. Windowing, Statistical & Analytical processing

6. Data Publishing & Consumption - Enablement of the Cleansed, transformed and analysed data as a Data Product.
a. Discovery,
b. Outbound/Egress,
c. Reports/exports
d. Schema migration
'''

#EXTRACTION PART#
print("***************1. Data Munging *********************")
print("""
1. Data Munging - Process of transforming and mapping data from Raw form into Tidy format with the
intent of making it more appropriate and valuable for a variety of downstream purposes such for analytics/visualizations/analysis/reporting
a. Passive - Data Discovery (EDA) - Exploratory Data Analysis - Performing (Data Exploration) exploratory data analysis of the raw data to identify the attributes(columns/datatype) and patterns (format/sequence/alpha/numeric).
#Understand the data in its own format (read all attributes/columns as string) - total rows, total columns (statistics)
#Understand the data by applying inferschema structure - what is the natural datatypes
#Apply Custom structure using the inferred schema - copy the above columns and datatype into excel and get structure, GenAI (create a structtype with total 100 columns with all as string)   
#If the data is not matching with my structure then apply reject algorithm
#Identify Nulls, Duplicates, Datatypes, formats etc.,
#Summarize the overall pattern of data
b. Active - Data Structurizing - Combining Data + Schema Evolution/Merging/Melting (Structuring) - 
c. Active - Validation, Cleansing, Scrubbing (Preprocessing, Preparation, Validation) - Identifying and filling gaps & Cleaning data to remove outliers and inaccuracies
Cleansing (removal of unwanted datasets eg. na.drop),
Scrubbing (convert of raw to tidy na.fill or na.replace)
d. Standardization - Column name/type/order/number of columns changes/Replacement & Deletion of columns to make it in a usable format
""")

'''1. Data Munging - Process of transforming and mapping data from Raw form into Tidy format with the
intent of making it more appropriate and valuable for a variety of downstream purposes such for analytics/visualizations/analysis/reporting
a. Data Discovery (EDA) (attributes, patterns/trends) - Exploratory Data Analysis - Performing (Data Exploration) exploratory data analysis of the raw data to identify the attributes (columns/datatype) and patterns (format/sequence/alpha/numeric).
#Understand the data in its own format (read all attributes/columns as string) - total rows, total columns 
#Understand the data by applying inferschema structure - what is the natural datatypes
#Apply Custom structure using the inferred schema - copy the above columns and datatype into excel and get structure, GenAI (create a structtype with total 100 columns with all as string)   
#If the data is not matching with my structure then apply reject algorithm
#Passively Identify Nulls (na.drop, sql(is null)), constraints (structtype nullable=False), 
De-Duplication & Prioritization (distinct, dropDuplicates()),row_number(), Datatypes is not as per the expectation because of the data format hence (rlike or upper()=lower() -> regexp_replace -> cast ) etc.,
Summarize (statistics) the overall trend of data'''

#Define Spark Session Object or sparkContext+sqlContext for writing spark sql program
from pyspark.sql import *
spark=SparkSession.builder.master("local[*]").appName("WE43 ETL BB2 Application").enableHiveSupport().getOrCreate()

#a. Passive - Data Discovery/understanding/analysis (EDA)
#Understand the data in its own format (read all attributes/columns as string) - total rows, total columns
df_raw1=spark.read.csv("file:///home/hduser/sparkdata/custsmodified").toDF("cid","fname","lname","age","prof")
print(df_raw1.count())#10005
df_raw1.printSchema()

#Understand the data by applying inferschema structure - what is the natural datatypes
df_raw1=spark.read.csv("file:///home/hduser/sparkdata/custsmodified",inferSchema=True).toDF("cid","fname","lname","age","prof")
print(df_raw1.count())#10005
df_raw1.printSchema()
#Findings:
#Cid and age are not having int values alone as expected
#We are going to use the above structure to define the custom structure


#Apply custom schema using infer schema identified columns and datatype, then apply different modes for different data exploration
#Syntax customsch=StructType([StructField("colname",StringType(),True)])
#cid: string, fname: string, lname: string, age: int, prof: string
from pyspark.sql.types import StructType,StructField,StringType,IntegerType
customsch=StructType([StructField("cid",IntegerType(),True),StructField("fname",StringType(),True),StructField("lname",StringType(),True),StructField("age",IntegerType(),True),StructField("prof",StringType(),True)])

#Let us identify howmuch percent of malformed data is there in our dataset (data is not as per the structure, number of columns are less than the structure defined)
df_raw1=spark.read.csv("file:///home/hduser/sparkdata/custsmodified",schema=customsch,mode="dropmalformed")
#Total records in the file is 10005
#After dropping malformed data is 10000
df_raw1.count()#10005 (the count behavior is to find the count of original data ignoring dropmalformed)
len(df_raw1.collect()) #10000 (workaround is to do collect and length)
#Findings:
# 5 malformed data we have
# We don't know which are the records dropped?

#Identify only the corrupted data alone and I need to analyse/log them or send it to our source system to get it corrected
#Do a custom functionality checks to identify the malformed data (costly effort)
df_raw2=spark.read.csv("file:///home/hduser/sparkdata/custsmodified",inferSchema=True).toDF("cid","fname","lname","age","prof")
df_raw2.where("upper(cid)<>lower(cid)").show()
df_raw2.where("""rlike(age,'[-]')==True""").show()

from pyspark.sql.functions import *
df_raw2=spark.read.csv("file:///home/hduser/sparkdata/custsmodified",inferSchema=True,sep='~')
df_raw2.where(size(split("_c0",","))!=5).show(20,False)

#Findings:
# 5 malformed data we have
# We don't know which are the records dropped?
# 2 records due to cid having string, 1 records due to age is not in proper format, 2 records due to size of column is < or > 5

#or
#Why not we can look for some options, that spark can give us to identify the malformed data easily (simple approach)
from pyspark.sql.types import StructType,StructField,StringType,IntegerType
customschmalformed=StructType([StructField("cid",IntegerType(),True),StructField("fname",StringType(),True),StructField("lname",StringType(),True),StructField("age",IntegerType(),True),StructField("prof",StringType(),True),StructField("malformed_data",StringType(),True)])
df_raw1=spark.read.csv("file:///home/hduser/sparkdata/custsmodified",schema=customschmalformed,mode="permissive",columnNameOfCorruptRecord="malformed_data")
df_raw1.show(20,False)

#Findings:
# 5 malformed data we have
# We don't know which are the records dropped?
# 2 records due to cid having string, 1 records due to age is not in proper format, 2 records due to size of column is < or > 5

#df_raw1.where("malformed_data is not null").select("malformed_data").show()
'''
pyspark.sql.utils.AnalysisException: Since Spark 2.3, the queries from raw JSON/CSV files are disallowed when the
referenced columns only include the internal corrupt record column
(named _corrupt_record by default). For example:
spark.read.schema(schema).csv(file).filter($"_corrupt_record".isNotNull).count()
and spark.read.schema(schema).csv(file).select("_corrupt_record").show().
Instead, you can cache or save the parsed results and then send the same query.
For example, val df = spark.read.schema(schema).csv(file).cache() and then
df.filter($"_corrupt_record".isNotNull).count().
'''
df_raw1.cache()
df_malformed=df_raw1.where("malformed_data is not null").select("malformed_data")
df_malformed.show()

#Reject Strategy to store this data in some audit table or audit files and send it to the source system or we do analysis/reprocessing of these data
df_malformed.write.csv("file:///home/hduser/sparkrejectnaren")

#Good Data
df_raw1.cache()
df_cleansed_data=df_raw1.where("malformed_data is null").drop("malformed_data")
df_cleansed_data.count()#10000

#Identify & Analyse Nulls, constraints, Duplicates(row/column), Datatypes(cid), formats(age) etc.,
#Null handling on the key column (null in any, all, subset, constraint)
df_raw2=spark.read.csv("file:///home/hduser/sparkdata/custsmodified",inferSchema=True).toDF("cid","fname","lname","age","prof")

from pyspark.sql.types import StructType,StructField,StringType,IntegerType
customsch=StructType([StructField("cid",IntegerType(),True),StructField("fname",StringType(),True),StructField("lname",StringType(),True),StructField("age",IntegerType(),True),StructField("prof",StringType(),True)])
df_raw1=spark.read.csv("file:///home/hduser/sparkdata/custsmodified",schema=customsch,mode="permissive")
df_raw1.show(20,False)

#DSL builtin functions
#We are using "na" which is a pure null handling function
df2=df_raw1.na.drop()#Drop all the records with any column contains null (by default how=any is applied)
df3=df_raw1.na.drop(how="all")#Drop the records with all column contains null
df3=df_raw1.na.drop(how="any",subset=["cid","prof"])#Drop the records with cid,prof column if any one contains null
df3=df_raw1.na.drop(how="all",subset=["cid","prof"])#Drop the records with cid,prof column both contains null

#DSL regular functions
#Which are the data dropped using df_raw1.na.drop(how="all",subset=["cid","prof"])
df3=df_raw1.where("cid is null and prof is null")
df3.show()

#Aspirant's Equivalent SQL:
#One Example, but try with all other options used in DSL above...
df_raw1.createOrReplaceTempView("df_raw1view")
spark.sql("select * from df_raw1view where cid is not null and fname is not null and lname is not null and age is not null and prof is not null").count()
#10005-df_raw1.na.drop().count()

#Null Constraints (Rule I want to putforth on the given column which should not have nulls)
#Sanjay's Question?
#Proactive Interview Question? Also we provided Nullable as False. That will also create an issue right, why spark is not failing? This is open limitation in spark, and we have a workaround for it
#Nullable False represents - if we get null in this column fail our program from further processing
#Workaround - Nullable can't be directly applied on DF, But it can be applied on RDD, and the RDD can be converted back to DF.
#https://issues.apache.org/jira/browse/SPARK-10848

#One scope reduction of spark is the not null constraint of nullable false cannot be applied at the time of DF definition, we need to have a workaround
#Not null constraint is applied now --- ValueError: field cid: This field is not nullable, but got None
from pyspark.sql.types import StructType,StructField,StringType,IntegerType
customsch=StructType([StructField("cid",IntegerType(),False),StructField("fname",StringType(),True),StructField("lname",StringType(),True),StructField("age",IntegerType(),True),StructField("prof",StringType(),True)])
df_raw1=spark.read.csv("file:///home/hduser/sparkdata/custsmodified",schema=customsch,mode="permissive")
df_raw1.printSchema()
#df_raw1=df_raw1.na.drop(how="any",subset=["cid"])
df_raw2=df_raw1.rdd.toDF(customsch)#program will fail if null come in the significant column cid
df_raw2.show()

df_raw2=df_raw1.na.fill(-1,subset=["cid"]).rdd.toDF(customsch)#removing/fill nulls to apply contraint (just to make my program work)

#Findings:
#There are complete record itself contains null
#most of the columns randomly having nulls
# There are records with cid coming as null, which is not acceptable, we can adjust for sometime using na.drop to remove those records and apply the constraint

#Duplicates - Identification of duplicate records/column values
from pyspark.sql.types import StructType,StructField,StringType,IntegerType
customsch=StructType([StructField("cid",IntegerType(),False),StructField("fname",StringType(),True),StructField("lname",StringType(),True),StructField("age",IntegerType(),True),StructField("prof",StringType(),True)])
df_raw1=spark.read.csv("file:///home/hduser/sparkdata/custsmodified",schema=customsch,mode="permissive")

#Record level duplicates
#DSL
df_raw1.count() - df_raw1.distinct().count()#2 duplicates are there in the data
df_raw1.count() - df_raw1.dropDuplicates().count()#2 duplicates are there in the data

#or
#SQL
#Want to analyse which are the duplicate records
df_raw1.createOrReplaceTempView("df_raw1view")#SQL to identify dup records
spark.sql("select cid,fname,lname,age,prof,count(1) from df_raw1view group by cid,fname,lname,age,prof having count(1)>1 order by cid").show()

#Colum level de-duplicates with prioritization
df_dedup=df_raw1.dropDuplicates(subset=["cid"])
df_dedup.where("cid=4000003").show()

#Deduplication with Prioritization
#If I want to retain only young aged customer
df_dedup=df_raw1.sort("age",ascending=True).dropDuplicates(subset=["cid"])#Drop duplicates retains only the first unique value and drop rest of all dups
df_dedup.where("cid=4000003").show()
#If I want to retain only old aged customer
df_dedup=df_raw1.sort("age",ascending=False).dropDuplicates(subset=["cid"])
df_dedup.where("cid=4000003").show()

#Regular DSL function to achieve same result - If I want to retain only young aged customer
#df_raw1.withColumnRenamed("age","mark").where("cid=4000003").select("*",).show()
from pyspark.sql.window import Window
df_dedup=df_raw1.select("*",row_number().over(Window.partitionBy("cid").orderBy("age")).alias("rank"))
df_dedup.where("cid=4000003").show()

#Regular DSL function to achieve same result - If I want to retain only old aged customer
df_dedup=df_raw1.select("*",row_number().over(Window.partitionBy("cid").orderBy(col("age").desc())).alias("rno"))
df_dedup.where("cid=4000003").show()
df_dedup_final=df_dedup.where("rno=1").drop("rno")
df_dedup_final.where("cid=4000003").show()

#Interview Question: Difference between distinct and dropduplicates in pyspark DSL
#Answer -
# Distinct is used to do row level deduplication
# DropDuplicates is used to do row & column level deduplication

#Interview Question:
# How do we eliminate duplicate data from a given dataset/table using analytical/window functions?
# What is the 2nd maximum salary/ maximum sales happened in the business?
#Answer -
# We can achieve the result using analytical-window functions

#Aspirant's Equivalent SQL:
#in last 3 months i have attended 20+ interviews , in all i got this rank question and max of 3 sales question. now very clear, thank u
df_raw1.withColumnRenamed("age","mark").createOrReplaceTempView("v1")
#Deduplication
spark.sql("""select cid,fname,lname,mark,prof from
          (select *,row_number()over(partition by cid order by mark desc) rank from v1 where cid<=4000010)temp
          where rank=1""").show()
#Find the students who got 2nd highest mar
spark.sql("""select cid,fname,lname,mark,prof from
          (select *,row_number()over(partition by cid order by mark desc) rank from v1 where cid<=4000010)temp
          where rank=2""").show()

#Datatypes(cid), formats(age) check on the dataframe
df_raw2=spark.read.csv("file:///home/hduser/sparkdata/custsmodified",inferSchema=True).toDF("cid","fname","lname","age","prof")
df_raw2.where("upper(cid)<>lower(cid)").show()
#or
df_raw2.where("""rlike(cid,'[a-z|A-Z]')==True""").show()
df_raw2.where("""rlike(age,'[-]')==True""").show()

#Findings:
# 2 records due to cid having string, 1 records due to age is not in proper format

#Aspirant's Equivalent SQL:


#Summarize (statistics) the overall trend of data
df_raw2.describe().show()
df_raw2.summary().show()

#Findings:
#Summary and describe help us understand at every column how many
#count of null values are there, mean(mid value) of a given int columns (understand the distribution of data)
#Percentail of the distribution of data

#Aspirant's Equivalent SQL:

#Identify & Analyse Nulls, Duplicates(row/column), Datatypes(cid), formats(age) etc.,

#Completed (Passive) a. Data Discovery (every layers ingestion/transformation/consumption) - (Data Governance (security) - Tagging, categorization, classification) (EDA) (Data Exploration) - Performing an (Data Exploration) exploratory data analysis of the raw data to identify the attributes and patterns.

print("Active - b. Combining/Melting Data + Schema Evolution/Merging (Structuring)")
#Data Structurizing - Combining Data (diff dir, subdir, filenames) +
# Merging (sameday of diff structure) (unionByName+allowmissingfields) +
# Schema Evolution (different days of diff structure) (source df -> staging(orc/parquet) -> read.orc(mergeSchema) -> hive)

'''
[hduser@localhost sparkdata]$ cp -R custdata custdata1
[hduser@localhost sparkdata]$ mkdir custdata/dir1
[hduser@localhost sparkdata]$ mkdir custdata/dir2
[hduser@localhost sparkdata]$ cd custdata
[hduser@localhost custdata]$ cp custsmodified1 dir1/custsmodified4
[hduser@localhost custdata]$ cp custsmodified1 dir1/custsmodified5
[hduser@localhost custdata]$ cp custsmodified1 dir2/custsmodified6
[hduser@localhost custdata]$ cp custsmodified1 dir2/custsmodified7
'''

df_raw2=spark.read.csv("file:///home/hduser/sparkdata/custdata",inferSchema=True,header=True).toDF("cid","fname","lname","age","prof")
df_raw2.count()#61 (after eliminating header 61-4=57)
#Above function will consider all the files with any filename or pattern

print("b.1. Combining Data - Reading from one path contains multiple pattern of files")
#Thumb rule/Caveat is to have the data in the expected structure with same number and order of columns
df_raw2=spark.read.csv("file:///home/hduser/sparkdata/custdata",inferSchema=True,pathGlobFilter="custsmodified[0-9]",header=True).toDF("cid","fname","lname","age","prof")

#Above function will consider all the files with any filename or pattern
print("b.2. Combining Data - Reading from a multiple different paths contains multiple pattern of files")
df_raw2=spark.read.csv(["file:///home/hduser/sparkdata/custdata","file:///home/hduser/sparkdata/custdata1"],inferSchema=True,pathGlobFilter="custsmodified[0-9]").toDF("cid","fname","lname","age","prof")
df_raw2.count()#82 count as expected

#wc -l /home/hduser/sparkdata/custdata1/custsmodified[0-9] /home/hduser/sparkdata/custdata/custsmodified[0-9]

print("b.2. Combining Data - Reading from a multiple sub paths contains multiple pattern of files")
df_raw2=spark.read.csv(["file:///home/hduser/sparkdata/custdata","file:///home/hduser/sparkdata/custdata1"],
                       inferSchema=True,pathGlobFilter="custsmodified[0-9]",recursiveFileLookup=True,header=True).toDF("cid","fname","lname","age","prof")
df_raw2.count()#162 count as expected

#wc -l /home/hduser/sparkdata/custdata1/custsmodified[0-9] /home/hduser/sparkdata/custdata/*/custsmodified[0-9] /home/hduser/sparkdata/custdata/custsmodified[0-9]

print("b.3. Schema Merging (Structuring) leads to Schema Evolution - Schema Merging data with different structures (we know the structure of both datasets)")
df_raw1=spark.read.csv("file:///home/hduser/sparkdata/custdata/custsmodified1",inferSchema=True,header=True)
df_raw2=spark.read.csv("file:///home/hduser/sparkdata/custdata/custsmodified2",inferSchema=True,header=True)
df_raw3=spark.read.csv("file:///home/hduser/sparkdata/custdata/custsmodified3",inferSchema=True,header=True)

#Manual intervention (may require on the data if we receive from different sources like DB/FS/Cloud...)
df_raw1_raw2=df_raw1.withColumn("city",lit(None)).union(df_raw2)
df_raw3_corrected=df_raw3.withColumn("age",lit(None)).select("custid","fname","lname","age","prof","city")
#Set operation (union, subtract, intersect)
df_all=df_raw1_raw2.union(df_raw3_corrected)
df_all.show(100)


#or If we get all the data from one source which has different schema
#Automatically we can handle using unionByName & allowMissingColumns
df_all=df_raw1.unionByName(df_raw2,allowMissingColumns=True).unionByName(df_raw3,allowMissingColumns=True)
df_all.show(100)

#When I want to extract data in a proper structure with the right column mapping


#Aspirant's Equivalent SQL: df_all (manual)
#Represent all DFs as Tempviews
#sql("select col1,col2 from view1 union select col1,col2,null age from view2 union ...")
spark.sql("""select custid,fname,lname,     age,prof,null as city from v1 
             union 
             select custid,fname,lname,     age,prof,        city from v2 
             union 
             select custid,fname,lname,null age,prof,        city from v3""").show(1000)


#Proactive Interview Discussion:
# We receive data from source systems in a evolving schema (column numbers, order, types) may change/increase/decrease on & off, handling this required
# manual intervention, hence we did schema evolution by using (costly option unionbyname or optimized feature of orc/parquet interim storage and used mergeSchema)
#Eg. Scenario: I have already data with 5 columns a,b,c,e,d till yesterday and from today i am receiving data from the source with 6 columns like a,c,b,d,g,h
print("b.4. Schema Evolution (Structuring) - source data is evolving with different structure")

#Schema merging can leads to schema Evolution also
df_day1=spark.read.csv("file:///home/hduser/sparkdata/custsmodified_evolve_day1",inferSchema=True,header=True)
df_day2=spark.read.csv("file:///home/hduser/sparkdata/custsmodified_evolve_day2",inferSchema=True,header=True)
df_raw1.write.mode("overwrite").csv("/user/hduser/custinfocsv",header=True,sep='~')
df_raw2.write.mode("append").csv("/user/hduser/custinfocsv",header=True,sep='~')
df_all_datalake_data=spark.read.csv("/user/hduser/custinfocsv",header=True,sep='~')
df_all_datalake_data.show(30)

#below is not good to do, because the data in the datalake is not as per the structure we got today
df_day3=spark.read.csv("file:///home/hduser/sparkdata/custsmodified_evolve_day3",inferSchema=True,header=True)
df_raw3.write.mode("append").csv("/user/hduser/custinfocsv",header=True,sep='~')

#Either I have to go with unionByName  (costly) or by using orc/parquet as a storage layer (optimized way)

#If I go with unionByName on the data that i received so far (1 year) with the today data and overwrite all data in target and I get the result achieved, but costly.
df_all_days_target_data=spark.read.csv("/user/hduser/custinfocsv/",inferSchema=True,header=True,sep='~')#Bringing all historical data to spark memory (not optimized)
df_day3=spark.read.csv("file:///home/hduser/sparkdata/custsmodified_evolve_day3",inferSchema=True,header=True)
df_all=df_all_days_target_data.unionByName(df_day3,allowMissingColumns=True)
df_all.show(100)#Result is correct, but costly way to achive the result
#df_all.write.mode("overwrite").csv("/user/hduser/custinfocsv",header=True,sep='~')#costly effort

#or

#Preferred way to achive schema evolution is by using orc/parquet formats (day by day sturcture of data may change)

df_day1=spark.read.csv("file:///home/hduser/sparkdata/custsmodified_evolve_day1",inferSchema=True,header=True)
df_day1.write.mode("overwrite").orc("/user/hduser/custinfoorc")#First I stage the data in the raw/staging layer
#or df_day1.write.mode("overwrite").parquet("/user/hduser/custinfoparquet")#First I stage the data in the raw/staging layer
df_day1_orc=spark.read.orc("/user/hduser/custinfoorc",mergeSchema=True)
df_day1_orc.show(100)


df_day2=spark.read.csv("file:///home/hduser/sparkdata/custsmodified_evolve_day2",inferSchema=True,header=True)
df_day2.write.mode("append").orc("/user/hduser/custinfoorc")#First I stage the data in the raw/staging layer
#or df_day1.write.mode("overwrite").parquet("/user/hduser/custinfoparquet")#First I stage the data in the raw/staging layer
df_day12_orc=spark.read.orc("/user/hduser/custinfoorc",mergeSchema=True)#merge schema will take care of reordering the columns, adding/removing the columns on the entire data
df_day12_orc.show(100)

df_day3=spark.read.csv("file:///home/hduser/sparkdata/custsmodified_evolve_day3",inferSchema=True,header=True)
df_day3.write.mode("append").orc("/user/hduser/custinfoorc")#First I stage the data in the raw/staging layer
#or df_day1.write.mode("overwrite").parquet("/user/hduser/custinfoparquet")#First I stage the data in the raw/staging layer
df_day123_orc=spark.read.orc("/user/hduser/custinfoorc",mergeSchema=True)
df_day123_orc.show(100)

#One challenge in the above scenario is the columns with same name should be of same data type

#Rafeeq's question:
#I got requirement from api to load,
# but in source schema is dynamic, so not sure how to load in targetable if schema is dynamic. so asked this question
#API -> orc/parquet -> drop hive table -> read.parquet(mergeSchema=True)-> hive table
df_day1.write.mode("append").parquet("/user/hduser/parquetevolve")
spark.sql("drop table hive_evolve")
spark.read.parquet("/user/hduser/parquetevolve",mergeSchema=True).write.mode("overwrite").saveAsTable("hive_evolve")
df_day2.write.mode("append").parquet("/user/hduser/parquetevolve")
spark.sql("drop table hive_evolve")
spark.read.parquet("/user/hduser/parquetevolve",mergeSchema=True).write.mode("overwrite").saveAsTable("hive_evolve")
df_day3.write.mode("append").parquet("/user/hduser/parquetevolve")
spark.sql("drop table hive_evolve")
spark.read.parquet("/user/hduser/parquetevolve",mergeSchema=True).write.mode("overwrite").saveAsTable("hive_evolve")

#from api source/SM/DB/  in batch/streaming data for schema evaluation,
# we have to go with scd1  ? is my understanding correct yes/no.
#yes - if my business don't wanted to maintain history - current designation of my customer (scd1)
#No - if my business wanted to maintain history - current & previous designations of my customer (scd2)


print("c.1. Active - Data Preparation/Preprocessing - Validation (active)- DeDuplication, Prioritization - distinct, dropDuplicates, windowing (rank/denserank/rownumber), groupby having")

#Validations
#Datatypes(cid), formats(age) check on the dataframe
df_raw1=spark.read.csv("file:///home/hduser/sparkdata/custsmodified",inferSchema=True).toDF("cid","fname","lname","age","prof")
df_raw1.where("upper(cid)<>lower(cid)").show()
#or
df_raw1.where("""rlike(cid,'[a-z|A-Z]')==True""").show()
df_raw1.where("""rlike(age,'[-]')==True""").show()


#De Duplication
#Record level duplicates
#DSL
df_dedup_row=df_raw1.distinct()

#Colum level de-duplicates with prioritization
df_dedup_col=df_dedup_row.dropDuplicates(subset=["cid"])

#Deduplication with Prioritization
#If I want to retain only young aged customer
df_dedup_priority=df_dedup_row.sort("age",ascending=True).dropDuplicates(subset=["cid"])#Drop duplicates retains only the first unique value and drop rest of all dups


#Regular DSL function to achieve same result - If I want to retain only young aged customer
#df_raw1.withColumnRenamed("age","mark").where("cid=4000003").select("*",).show()
from pyspark.sql.window import Window
df_dedup_priority=df_dedup_row.select("*",row_number().over(Window.partitionBy("cid").orderBy("age")).alias("rank"))
df_dedup_priority.where("cid=4000003").show()

#Aspirant's Equivalent SQL: try mixing sql and dsl

print("c.2. Active - Data Preparation/Preprocessing/Validation (Cleansing & Scrubbing) - "
      "Cleaning data to remove outliers and inaccuracies & Identifying and filling gaps ")
#We are going to use na function to achieve both cleansing and scrubbing
#Cleansing - uncleaned vessel will be thrown (na.drop is used for cleansing)
#df_raw=spark.read.csv("file:///home/hduser/sparkdata/custsmodified_na",inferSchema=True,header=True).toDF("cid","lname","fname","age","prof")

spark.conf.set("spark.sql.shuffle.partitions","5")

#Cleansing - cleaning unwanted data
df_dedup_priority.where("cid is null").show()#validation
df_dedup_priority_cleanup1=df_dedup_priority.na.drop("all")
df_dedup_priority_cleanup1.where("cid is null").show()#validation

df_dedup_priority_cleanup2=df_dedup_priority_cleanup1.na.drop(how="any",subset=['age','fname'])#any one of the column contain nulls means, drop it
df_dedup_priority_cleanup2.count()#The DF with age or fname contains null will be dropped
df_dedup_priority_cleanup3=df_dedup_priority_cleanup2.na.drop(thresh=3,subset=['age','fname','lname','prof'])#Threshold of Minimum 4 columns should not have nulls

#don't try to achieve thresh using SQL, which is complex
#finding here is - usage of SQL or DSL which one is better? In this case usage of DSL is better
#Aspirant's Equivalent SQL:


#Scrubbing (repairing) - repair and reusable vessel (na.fill and na.replace is used for scrubbing or filling gaps)
df_dedup_priority_cleanup3.count()#9996 rows
#na.fill & na.replace
df_dedup_priority_cleanup3.where("lname is null").show()
'''
+-------+-------+-----+---+------+
|    cid|  fname|lname|age|  prof|
+-------+-------+-----+---+------+
|4000009|Malcolm| null| 39|Artist|
+-------+-------+-----+---+------+
'''
df_dedup_priority_cleansed_scrub1=df_dedup_priority_cleanup3.na.fill("na",subset=["lname"])

#df_dedup_priority_scrub1 - Now the cleansed & scrubbed data is having no age or fname with nulls and lname filled with na for null values
#out of age,fname,lname,prof minimum 3 columns will have values

#Scrubbing the DF by Replacing prof with IT as data engineer and writer with editor
dict_to_replace={'IT':'Data Engineer','Writer':'Editor'}
df_dedup_priority_cleansed_scrub2=df_dedup_priority_cleansed_scrub1.na.replace(dict_to_replace,subset=["prof"])

#df_dedup_priority_scrub2 df is cleansed and scrubbed DF

#Aspirant's Equivalent SQL:
#sql("select coalesce(cid,-1) from view1").show()

print("d.1. Data Standardization : Data/Columns(name/type) - Column re-order/number of columns changes (add/remove/Replacement/Renaming)  to make it in a understandable/usable format")
#Apply few column functions to do column - reordering, rename, removal, replacement, add, changing type of the columns
#To make this data more meaningful we have by
#add the sourcesystem from where we received the data from - withColumn
#rename the columns like cid as custid, prof as profession - withColumnRenamed/alias
#replace the columns like fname with lname from the dataframe - withColumn
#remove the columns like lname from the dataframe - drop
#typecast like custid as int and age as int - cast
#reorder the columns like custid,fname,profession,age - select/withColumn
#Add a hardcoded value - lit
#Identify a given value is a column - col

#How to use important DSL column management functions like -
# withColumn("new_colname",Column(lit/col/functionoutput())) - add/replace/reorder
# withColumnRenamed("existing_column","new_column_name")
# drop
from pyspark.sql.functions import *
#Standardization 1 - Ensure to add (withColumn) source system for all our data
df_col_added=df_dedup_priority_cleansed_scrub2.withColumn("sourcesystem",lit("Marketing"))

#Doing some analysis using withColumn function along with adding a column on the dataframe profession column to check how many words we have
df_dedup_priority_cleansed_scrub2.withColumn("profession_len",size(split("prof",' '))).sort("profession_len",ascending=False).show(1)

#Standardization 2 - Ensure to rename (withColumnRenamed) the columns with the abbrevation extended
df_col_added_renamed=df_col_added.withColumnRenamed("cid","custid").withColumnRenamed("prof","profession")

df_col_added_renamed_analysed=df_col_added_renamed.withColumn("profession_len",size(split("profession",' '))).sort("profession_len",ascending=False)

#Remove columns - removing post analysis columns or some source columns that is not needed
#Standardization 3 - Ensure to remove (drop) the columns that we found unwanted to propogate further
df_col_added_renamed_analysed_removed=df_col_added_renamed_analysed.drop("profession_len")

#replace profession with upper(profession)
#Standardization 4 - Ensure to have the profession replaced/changed (withColumn) to upper case
df_col_added_renamed_analysed_removed_upper_case=df_col_added_renamed_analysed_removed.withColumn("profession",upper("profession"))

#Want to add profession_upper in the above dataframe? use withColumn to derive a column from another column
df_col_added_renamed_analysed_removed_upper_case.withColumn("profession_upper",upper("profession")).show()

#Typecasting - Changing the type of the column appropriately - custid int, age int
#Standardization 5 - Ensure to redefine the dataframe with the appropriate datatypes type casting
df_col_added_renamed_analysed_removed_upper_case.printSchema()
df_col_added_renamed_analysed_removed_upper_case_casted=df_col_added_renamed_analysed_removed_upper_case.withColumn("custid",col("custid").cast("int")).withColumn("age",col("age").cast("int"))

#Reordering of the column custid,fname,profession,age
df_col_added_renamed_analysed_removed_upper_case_casted_reorder=df_col_added_renamed_analysed_removed_upper_case_casted.\
    select("custid","fname","lname","profession","age","sourcesystem")

#Rename the above Dataframe df_dedup_priority_cleansed_scrub2 to df_dedup_priority_cleansed_scrub_col_added_renamed_analysed_removed_upper_case_casted_reorder
df_dedup_priority_cleansed_scrub_col_added_renamed_analysed_removed_upper_case_casted_reorder=df_col_added_renamed_analysed_removed_upper_case_casted_reorder
#or
df_dedup_cleanse_scrub_add_ren_rem_cast_reorder=df_col_added_renamed_analysed_removed_upper_case_casted_reorder
#or
df_munged=df_col_added_renamed_analysed_removed_upper_case_casted_reorder

#Aspirant's SQL Equivalent - SQL is more comfortable to achive all these Data Standardization
#Add, remove, replace, rearrange, renaming

df_dedup_priority_cleansed_scrub2.createOrReplaceTempView("view1")
spark.sql("select cid,fname,lname,age,prof,'retail' as source,current_date() as datadt from view1")#add columns
spark.sql("select cid as custid,prof,lname as fname,lname,'retail' as source,current_date() as datadt from view1")#rearrange, replace...
#Add, remove, replace, rearrange, renaming
final_standardized_sql_df=spark.sql("select cast(cid as int) as custid,prof as profession,lname as fname,age,'retail' as source,current_date() as datadt from view1")

print("********************1. data munging completed****************")


'''2. Starting - Data Enrichment - Makes your data rich and detailed
a. Add, Remove, Rename, Modify/replace
b. split, merge/Concat
c. Type Casting, format & Schema Migration
'''

print("***************2. Data Enrichment (values)-> Add, Rename, merge(Concat), Split, Casting of Fields, Reformat, "
      "replacement of (values in the columns) - Makes your data rich and detailed *********************")

#Add Columns (select/withColumn) - for this pipeline, I needed the load date and the timestamp
df_munged_enriched1=df_munged.select("*",current_date().alias("load_dt"),current_timestamp().alias("load_ts"))#additional columns can be added anywhere
#or
df_munged_enriched1=df_munged.withColumn("load_dt",lit('2024-06-02')).withColumn("load_ts",current_timestamp())#additional columns added in the last

#Rename Columns (fname, lname)
df_munged_enriched2=df_munged_enriched1.withColumnRenamed("fname","firstname").withColumnRenamed("lname","lastname")

#Swapping Columns
#By renaming and reordering
df_munged_enriched2.where("custid=4001514").show()
df_munged_enriched3=df_munged_enriched2.withColumnRenamed("firstname","firstnametmp").withColumnRenamed("lastname","firstname").withColumnRenamed("firstnametmp","lastname")

#b. merge/Concat, split
#merge/concat - mailid creation (mailid column with fname.lname@inceptez.com)
df_munged_enriched4=df_munged_enriched3.withColumn("mailid",concat("firstname",lit("."),"lastname",lit("@inceptez.com")))

#Split - domain from the mail id (domain column with inceptez.com)
df_munged_enriched5=df_munged_enriched4.withColumn("splitcol",split("mailid",'@')).withColumn("domain",col("splitcol")[1])

#Remove Columns
df_munged_enriched6=df_munged_enriched5.drop("splitcol")

#try do the above with select
df_munged_enriched6=df_munged.select("custid",col("fname").alias("lastname"),col("lname").alias("firstname"),
                                     "profession","age","sourcesystem",current_date().alias("load_dt"),
                                     current_timestamp().alias("load_ts"))
df_munged_enriched6_2=df_munged_enriched6.select("*",concat("firstname",lit("."),"lastname",lit("@inceptez.com")).alias("mailid"))
df_munged_enriched6_3=df_munged_enriched6_2.select("*",split("mailid",'@')[1].alias("domain"))
df_munged_enriched6_3.show(3)#This is equivalent to the DSL created df_munged_enriched6

#Equivalent Aspirant's SQL:
# Example SQL FYR
# sql("select *,concat(fname,'.',lname,'@inceptez.com') as mailid from v1").show()

#c. Type Casting, format & Schema Migration
#Type casting
#We need to change the loaddt into date format
df_munged_enriched7=df_munged_enriched6.withColumn("load_dt",col("load_dt").cast("date"))#converting load_dt from string to date type

#Extraction & Reformatting
#Extraction/Derivation
df_munged_enriched8=df_munged_enriched7.withColumn("mth",month("load_dt")).withColumn("yr",year("load_dt"))#Deriving month and year from the load_dt

#Reformatting of the date from yyyy-MM-dd (default format) to MM/dd/yyyy
#date_format - This date function help us to change the date from default format of yyyy-MM-dd to any date format we needed
#to_date - This date function help us to change the date from any date format to the default format of yyyy-MM-dd

df_munged_enriched9=df_munged_enriched8.withColumn("load_dt",date_format("load_dt",'MM/dd/yyyy'))#yyyy-MM-dd to MM/dd/yyyy

#Reformatting2 of the date from MM/dd/yyyy to yyyy-MM-dd (default format)
df_munged_enriched_learning=df_munged_enriched9.withColumn("load_dt",to_date("load_dt",'MM/dd/yyyy'))#MM/dd/yyyy to yyyy-MM-dd


#Aspirant's SQL Equivalent

#2. Ending - Data Enrichment - Makes your data rich and detailed

#########################################################################################################################################

print("***************3. Data Customization & Custom Processing (custom Business logics) -> Apply User defined functions and utils/functions/modularization/reusable functions & reusable framework creation *********************")
print("Data Customization can be achived by using UDFs - User Defined Functions")
print("User Defined Functions must be used only if it is Inevitable (un avoidable), because Spark consider UDF as a black box doesn't know how to "
      "apply optimization in the UDFs - When we have a custom requirement which cannot be achieved with the existing built-in functions.")

#### LEARNING ######
#Few things to learn first: 2 Types of Function are builtin/predefined functions (eg. upper/concat/substring) or user defined functions
#Predefined functions are already available in multiple libraries like pyspark.sql.functions, pyspark.sql.types ...
#How to create User defined functions ?

#costly approach & not an advisable approach (use udfs if it is inevitable)
#a. create a python function with all custom functionality writtened (only works in the python environment and not ready for pyspark DF)
pythonfun1=lambda x:x.upper()

#b. Convert/promote the python function to user defined function (pyspark provided us udf function and we have to pass this python function to the udf function of pyspark)
from pyspark.sql.functions import udf

#c. we need to promote/convert the python function to udf function
#df_raw1.na.drop("any").withColumn("fname",pythonfun1("fname")).show(1)#not possible to use direct python function in DFs
pysparkreadyudffunction=udf(pythonfun1) #we need to promote/convert the python function to udf function
df2=df_raw1.na.drop("any").withColumn("fname",pysparkreadyudffunction("fname")).show(1)

#Or simply I can get a df2 created from df_raw1 easily using the built in/predefined functiondf2=df_raw1.na.drop("any").withColumn("fname",upper("fname"))
df2.show(2)

#Finding is - use builtin/predefined functions primarily (dont go for udfs if already we have equivalent builtins)

#### LEARNING COMPLETED ######

#I want to have a customization done on our dataframe using some functions
#Requirement is -> derive a agecat column from the age column from the DF df_munged_enriched9 with the rule of
#<=12 as 'childrens', >12 and <=19 as 'teens' and >19 as 'adults'

#3.a. How to create Python Functions & convert/Register it as a UDF to call these functions in the DSL/SQL Queries respectively

#DSL
#How to create a python function -> convert to UDF -> Use it in the DSL functions
#3.a.1. How to create Python Function:
def fun_agecat(ag):
    if ag<=12:
        return 'children'
    if ag>12 and ag<=19:
        return 'teenager'
    else:
        return 'adult'

#3.a.2. Convert the python function to pyspark UDF
#df_munged_enriched_customized=df_munged_enriched9.withColumn("agecat",fun_agecat("age"))#this will not work
from pyspark.sql.functions import udf
udf_fun_agecat=udf(fun_agecat)#converting python function to UDF for using the udf in Domain Specific Language

#3.a.3. Use the UDF in the Dataframe DSL
df_munged_enriched_customized=df_munged_enriched9.na.drop(subset=["age"]).withColumn("agecat",udf_fun_agecat("age"))#this will work

#validating the function output
df_munged_enriched9.na.drop(subset=["age"]).withColumn("agecat",udf_fun_agecat("age")).select("agecat").distinct().show()

#How to use udfs in SQL Query
#3.a.2. Convert the python function to pyspark UDF
#df_munged_enriched_customized=df_munged_enriched9.withColumn("agecat",fun_agecat("age"))#this will not work
spark.udf.register("sql_fun_agecat",fun_agecat)#converting python function to UDF for using the udf in Domain Specific Language

#3.a.3. Use the REGISTERED UDF in the SQL
df_munged_enriched9.na.drop(subset=["age"]).createOrReplaceTempView("view1")
df_munged_enriched_customized_sql=spark.sql("select *,sql_fun_agecat(age) as agecat from view1")
df_munged_enriched_customized_sql.select("agecat").distinct().show()

#Finding is....
#Data CUSTOMIZATION (if we can't use existing function)- I HAVE A CUSTOM REQUIREMENT: PYTHON FUNC -> CONVERTED/REGISTERED UDF -> USED DSL/SQL respectively

#Interview important Question:
#Have you used UDF in you project, if Yes, tell me the high level logic
#Have you used UDF in you project, if No, why? because udf is not suggested to use because it will degrade the performance for the below reason

#3.b. Go for UDFs if it is in evitable – Because the usage of UDFs will degrade the performance of your pipeline
#Drawback of creating UDF functions:
#1. Creating, testing, handling exception, validating is a timeconsuming and challenging process
#2. Important Drawback is Usage of UDFs will degrade the performace due to the following reasons
#   a.Custom functions are not by default serialize the data whereas builtin/predefined functions will serialze the data internally
#   b.Custom functions is a black box for Spark Optimizer and Spark (Catalyst) optimizer will not apply any optimization on the UDFs

#If we don't use udf, catalyst is applied for pushdown optimization
df2=df_raw1.na.drop()
df2.where("prof='Pilot'").show(1)
df2.where("prof='Pilot'").explain()#Catalyst is applied...
'''
== Physical Plan == "Catalyst optimizer - applying pushedfilter"
PushedFilters: [IsNotNull(_c4), EqualTo(_c4,Pilot)]
'''

fun1=lambda x:True if x=='Pilot' else False
spark.udf.register("udf_fun1",fun1)
df2.where("udf_fun1(prof)=True").show(2)
df2.where("udf_fun1(prof)=True").explain()

''''== Physical Plan == "Catalyst optimizer - Not applying pushedfilter"
PushedFilters: []
'''

#Findings:
#Go for UDFs if it is in evitable – Because the usage of UDFs will degrade the performance of your pipeline


print("***************4. Core Curation/Pre Wrangling - Core Data Processing/Transformation (Level1) (Pre Wrangling)  -> "
      "filter, transformation, Grouping, Aggregation/Summarization, Sorting, Analysis/Analytics (EDA) *********************")
#df_munged_enriched_customized - This DF has age with null and profession with blankspaces or nulls, and agecategory with adults & childrens

#Initial curation activities -

# We need to handle nulls in age, blankspaces/nulls in profession and transform/derive agecategory using case statement and without using udf
####################################################################################################################################################
df_munged_enriched_final=df_munged_enriched9

#Transformation on the munged & enriched dataframe to provide alternative solution for the customization
#How to avoid using udfs:
#unfamiliar DSL syntax
#Using case statement we are achieving the udf logic
df_munged_enriched_noncustomized_dsl_curated1=df_munged_enriched_final.\
    withColumn("agecat",when((col("age")<=12),lit('children')).
               when((col("age")>12) & (col("age")<=19),lit('teenager'))
               .otherwise(lit('adult')))

#Using coalesce function (equivalent to na.fill function) to convert null to 0
#Using case function to derive agecat column
df_munged_enriched_noncustomized_dsl_imputed_curated1=df_munged_enriched_final.withColumn("age",coalesce("age",lit(0))).withColumn("agecat",when((col("age")-4<=12),lit('children')).when(((col("age")-3>12) & (col("age")-5<=19)),lit('teenager')).otherwise(lit('adult')))

#Deriving the agecat_ind from the agecat column curated/derived above
#Enrichment (Derivation of KPIs (Key Performance Indicators))
df_munged_enriched_noncustomized_curated2=df_munged_enriched_noncustomized_dsl_curated1.withColumn("agecat_ind",upper(substring("agecat",1,1)))

#SQL Equivalent of case statement (works in all DBs/DWHs/Cloud DBs)
df_munged_enriched_final.createOrReplaceTempView("mun_enr_view")
df_munged_enriched_noncustomized_sql_curated1=spark.sql("""select *,
case when age<=12 then 'children' when age >12 and age<=19 then 'teenager' else 'adult' end as age_cat 
from (select custid,coalesce(age,0) as age,profession from mun_enr_view )view1""")#familiar syntax


#filter (after 2 levels of curation)
df_munged_enriched_noncustomized_curated3_filter1=df_munged_enriched_noncustomized_dsl_imputed_curated1.where("agecat in ('adult','teenager')")
df_munged_enriched_noncustomized_curated3_filter1.select("agecat").distinct().show()

################################################################LEARNING###################################################
#curation (after filter) # We need to know profession wise number of adult/teen customers we have in our system
df_munged_enriched_noncustomized_curated4_group_without_childrens=df_munged_enriched_noncustomized_curated3_filter1.\
    groupBy("profession").agg(count("age").alias("cnt"))
df_munged_enriched_noncustomized_curated4_group_without_childrens.show()#we will use this DF later for storing in some DB
#RECREATION AND FITNESS WORKER [1,1,1,1,1,1,1,1,1] -> RECREATION AND FITNESS WORKER,9
#RECREATION AND FITNESS WORKER [33,23,54,33,45,67,44,45,24] -> RECREATION AND FITNESS WORKER,9

#SQL Equivalent
spark.sql("""select profession,count(1) as cnt 
from (
    select *,
    case when age<=12 then 'children' when age >12 and age<=19 then 'teenager' else 'adult' end as age_cat 
    from mun_enr_view) temp1
where age_cat in ('adult','teenager')
group by profession
""").show()
################################################################LEARNING###################################################

#Grouping, Aggregation/Summarization

#One column grouping & aggregation

df_munged_enriched_noncustomized_curated_grouped3=df_munged_enriched_noncustomized_curated2.groupBy("agecat").agg(avg("age").alias("avg_age"))

#SQL Equivalent
df_munged_enriched_noncustomized_curated2.createOrReplaceTempView("view1")
spark.sql("select agecat,avg(age) from view1 group by agecat").show()

#multiple columns grouping & 1 column aggregation
df_munged_enriched_noncustomized_curated_grouped4=df_munged_enriched_noncustomized_curated2.groupBy("profession","agecat").agg(max("age").alias("max_age"))

#Aspirant's SQL Equivalent

df_raw1.createOrReplaceTempView("view1")
spark.sql("select prof,avg(age) from view1 group by prof").show()

#multiple columns grouping & multiple column aggregation
df_munged_enriched_noncustomized_curated_grouped5=df_munged_enriched_noncustomized_curated2.groupBy("profession","agecat").agg(min("age").alias("min_age"),max("age").alias("max_age"))

#Grouping, Aggregation/Summarization, Sorting
df_munged_enriched_noncustomized_curated_grouped6=df_munged_enriched_noncustomized_curated2.groupBy("profession","agecat").agg(min("age").alias("min_age"),max("age").alias("max_age")).orderBy("min_age",ascending=False)

df_munged_enriched_noncustomized_curated_grouped7=df_munged_enriched_noncustomized_curated2.groupBy("profession","agecat").agg(min("age").alias("min_age"),max("age").alias("max_age")).orderBy(["profession","agecat","max_age"],ascending=[False,True,False])

#Aspirant's SQL Query

#Ingress -> Raw zone ->(Munging - Enrichment - Customization - Curation)-> Curated Zone


print("*************** 5. Data Wrangling (Analytical Functionalities) - Complete Data Curation/Processing/Transformation (Level2)  -> "
      "Joins (Lookup, Lookup & Enrichment, Denormalization (schema modeling)), Windowing, Analytical, set operations "
      "Summarization (joined/lookup/enriched/denormalized) *********************")

#Curated Zone -> Wrangling/Analytical/Windowing/Join/Denorm/Group & Aggregation/Summarization... -> Discovery/Consumption/Egress layer

#Important Interview Question?
#Why Join is needed?
#Enrichment, lookup (identification/comparison), connecting datasets, corelated data analysis, expanding, viewing in a single view.
#What are different types of joins available in SQL?
#inner,full outer, left, right, semi, anti, self, cartisian/cross/product
#Categorize Joins (inner, outer(full, left, right), special joins (semi, anti), rare joins (self), avoided (cartisian/cross/product)

#What are joins you have so far used? inner, left, semi, anti

#Scenario is - I have dataset1 (10 rows) and dataset2 (15 rows) where 8 rows are matching between these 2 datasets now if we apply different types of joins on the given dataset
#what is the number of rows returned for every type of join that we use?

#Let's learn about the different types of joins using DF DSL?

spark.conf.set("spark.sql.shuffle.partitions","2")

df_raw1=spark.read.csv("file:///home/hduser/sparkdata/custsmodified").toDF("custid","fname","lname","age","prof")
df_left=df_raw1.where("custid in (4000001,4000002,4000004)").distinct()
df_right=df_raw1.where("custid in (4000001,4000011,4000012)").distinct()

#We are learning these 2 items
# 1. Concepts of joins - how different join works
# 2. how to write complete syntax of join of DFs using DSL functions

#DSL Syntax of writing joins
#Simple/Direct Syntax (inner join/natural/equi join)
#We will go with the below simple syntax
# 1. If we have same join column name between both DFs
# 2. If we have different data columns with different names between both DFs

df_right=df_raw1.where("custid in (4000001,4000011,4000012)").distinct().toDF("custid","fullname","lastname","custage","profession")
df_joined=df_left.join(df_right,on="custid",how="inner")

#Complete/Comprehensive Syntax
df_left=df_raw1.where("custid in (4000001,4000002,4000004)").distinct().toDF("custid","fname","lname","age","prof")
df_right=df_raw1.where("custid in (4000001,4000011,4000012,4000013)").distinct().toDF("cid","fname","lname","age","prof")

#Interview Question : If I have same column name between 2 dataframes joined, what issue it will give and how do you handle it?
#issue is ambiguous issue
#Handle by using alias by writing comprehensive syntax
from pyspark.sql.functions import *
df_joined=df_left.join(df_right,on=[col("custid")==col("cid")],how="inner")
df_joined.select("fname").show()
#pyspark.sql.utils.AnalysisException: Reference 'fname' is ambiguous, could be: fname, fname.

#Standard/Comprehensive/Complete way of writing join syntax
df_joined=df_left.alias("l").join(df_right.alias("r"),on=[col("l.custid")==col("r.cid")],how="inner")
df_joined.select("l.custid","l.lname","r.lname").show()

#If We are going with multiple join conditions, how do we handle
df_joined=df_left.alias("l").join(df_right.alias("r"),on=[(col("l.custid")==col("r.cid")) & (col("l.lname")==col("r.lname"))],how="inner")
df_joined.select("l.custid","l.lname","r.lname").show()

#Inner join DSL - Matching custid data both sides will be returned
df_joined=df_left.alias("l").join(df_right.alias("r"),on=[col("l.custid")==col("r.cid")],how="inner")
df_joined.select("l.custid","l.lname","r.cid","r.lname").show()

#Observations for inner join:
#returns only matching rows
#we need to use join conditions
#we don't have to mention the type of join, still inner/equi/natural join will happen


#Outer join DSL
#df_joined=df_left.alias("l").join(df_right.alias("r"),on=[col("l.custid")==col("r.cid")],how="leftouterjoin")
#Supported join types include: 'inner', 'outer', 'full', 'fullouter', 'full_outer', 'leftouter', 'left', 'left_outer',
# 'rightouter', 'right', 'right_outer', 'leftsemi', 'left_semi', 'semi', 'leftanti', 'left_anti', 'anti', 'cross'

#Leftouter
#syntax
df_joined=df_left.alias("l").join(df_right.alias("r"),on=[col("l.custid")==col("r.cid")],how="left")
df_joined.select("l.custid","l.lname","r.cid","r.lname").show()

#or
df_joined=df_left.alias("l").join(df_right.alias("r"),on=[col("l.custid")==col("r.cid")],how="leftouter")
df_joined.select("l.custid","l.lname","r.cid","r.lname").show()

#or
df_joined=df_left.alias("l").join(df_right.alias("r"),on=[col("l.custid")==col("r.cid")],how="left_outer")
df_joined.select("l.custid","l.lname","r.cid","r.lname").show()


#Observations for left join:
#returns matching rows in the left and right df with values
#returns matching rows in the LEFT df with values and un matching values in the RIGHT df with nulls

#Right
df_joined=df_left.alias("l").join(df_right.alias("r"),on=[col("l.custid")==col("r.cid")],how="right")
df_joined.select("l.custid","l.lname","r.cid","r.lname").show()
#Observations for right join:
#returns matching rows in the left and right df with values
#returns matching rows in the RIGHT df with values and un matching values in the LEFT df with nulls

#Full
df_joined=df_left.alias("l").join(df_right.alias("r"),on=[col("l.custid")==col("r.cid")],how="full")
df_joined.select("l.custid","l.lname","r.cid","r.lname").show()
#Observations for full join:
#returns matching rows in the left and right df with values
#returns matching rows in the LEFT df with values and un matching values in the RIGHT df with nulls
#returns matching rows in the RIGHT df with values and un matching values in the LEFT df with nulls

#Special Joins (Optimized & Subject join)

#left Semi: Semi join will compare left df with right df and will return only the matching data of left df alone
#Semi join returns Same result we are getting in Inner also ? yes, but only left side data alone will be displayed
df_joined=df_left.alias("l").join(df_right.alias("r"),on=[col("l.custid")==col("r.cid")],how="semi")
#df_joined.select("l.custid","l.lname","r.cid","r.lname").show()
df_joined.select("l.custid","l.lname").show()

#left Anti: Anti join will compare left df with right df and will return only the UN matching data of left df alone
df_joined=df_left.alias("l").join(df_right.alias("r"),on=[col("l.custid")==col("r.cid")],how="ANTI")
#df_joined.select("l.custid","l.lname","r.cid","r.lname").show()
df_joined.select("l.custid","l.lname").show()

#Right Anti join:Swap the DFs
df_joined=df_right.alias("r").join(df_left.alias("l"),on=[col("l.custid")==col("r.cid")],how="Anti")
df_joined.select("r.cid","r.lname").show()

#Self join DSL: Joining the dataset by itself is Self join, used for hierachical joining
#Interview Question: Have you used self join in your project?
#Yes, in the case of hirachical joins for identifying the customer who referred another customer to give referal offers
df_self=df_raw1.where("custid in (4000011,4000012,4000013,4000014,4000015)").distinct().toDF("cid","fname","lname","age","prof")
df_self1=df_self.withColumn("ref_cid",col("cid")-1)
df_self1.alias("l").join(df_self1.alias("r"),on=[(col("l.cid")==col("r.ref_cid"))]).selectExpr("concat(r.fname,' is referred by ',l.fname)").show(10,False)

#Cartesian Join (avoided) - returns the multiplication of the rows between df1 and df2 hence it is a cross product
df_joined=df_left.alias("l").join(df_right.alias("r"))
#Observations for Cartesian join:
#returns every permutation and combination of rows
#If we don't have join conditions to use directly

#SQL Query Equivalent
df_left.createOrReplaceTempView("df_leftv1")
df_right.createOrReplaceTempView("df_rightv1")
df_joined=spark.sql("select * from df_leftv1 join df_rightv1 on df_leftv1.custid=df_rightv1.custid")#Simple
df_joined=spark.sql("select l.*,r.* from df_leftv1 l join df_rightv1 r on l.custid=r.custid")#Comprehensive
df_joined=spark.sql("select l.*,r.* from df_leftv1 l join df_rightv1 r on l.custid=r.custid and l.lname=r.lname")#Comprehensive
df_joined.show()
df_joinedsql=spark.sql("select l.*,r.* from df_rightv1 l left join df_leftv1 r on l.custid=r.custid and l.lname=r.lname")#Comprehensive
df_joinedsql.show()


############################ JUST TRY THIS PIPELINE IN NO TIME ################################################################################
#We are going to introduce new dataset and it is going to undergo all possible stages of our overall data transformation
#Munging -> Enrichment -> Customization -> Curation (Pre Wrangling) -> Wrangling

#Munging
txns_raw=spark.read.csv("file:///home/hduser/sparkdata/txns",inferSchema=True).toDF("txnno","txndt","cid","amount","category","product","city","state","spendby")
from pyspark.sql.functions import *
#EDA
txns_raw.where("cid is null").show()
txns_raw.where("txnno is null").show()
txns_raw.printSchema()
txns_raw.where("upper(txnno)<>lower(txnno)").show()

#Cleansing & Scrubbing
txns_cleansed_scrubbed=txns_raw.na.drop(subset=["txnno"])

dict1={"lastvalue":'-1'}
txns_cleansed_scrubbed2=txns_cleansed_scrubbed.na.replace(dict1,subset=["txnno"]).withColumn("txnno",col("txnno").cast("int"))

#Enrichment
#We are reformatting the date and casting the date implicitly using to_date
txns_cleansed_scrubbed_enrich=txns_cleansed_scrubbed2.withColumn("txndt",to_date("txndt",'MM-dd-yyyy'))
txns_cleansed_scrubbed_enrich2=txns_cleansed_scrubbed_enrich.withColumn("source",lit("store data")).withColumn("loaddt",current_date())

#customization (We don't have any custom requirment)
#one scenario? Create a function to check the state (other than (California is free) add 5% tax for all other states)
# and add some percent of tax on the amount and create a new column tax_amount


#Curation Stage
#Let's Try to add more derived/enriched fields using some date functions
txns_cleansed_scrubbed_enrich_curated1=txns_cleansed_scrubbed_enrich2.select("*",month(col("txndt")).alias("mth"),
                                                                             year(col("txndt")).alias("year"),
                                                                             date_add(col("txndt"),1).alias("next_day"),
                                                                             date_sub(col("txndt"),1).alias("prev_day"),
                                                                             trunc(col("txndt"),'mm').alias("first_day_month"),
                                                                             last_day(col("txndt")).alias("last_day_month"),
                                                                             date_add(last_day(col("txndt")),1).alias("next_month_first_day"),
                                                                             date_sub(trunc(col("txndt"),'mm'),1).alias("prev_month_last_day"))

#Derive Indicators (KPIs)
txns_cleansed_scrubbed_enrich_curated2=txns_cleansed_scrubbed_enrich_curated1.withColumn("statecd",upper(substring(col("state"),1,3)))
txns_cleansed_scrubbed_enrich_curated3=txns_cleansed_scrubbed_enrich_curated2.\
    withColumn("firstday_ind",when(col("txndt")==col("first_day_month"),'Y').otherwise(lit('N'))).\
    withColumn("lastday_ind",when(col("txndt")==col("last_day_month"),'Y').otherwise(lit('N')))

#Filters
txns_cleansed_scrubbed_enrich_curated3_filter=txns_cleansed_scrubbed_enrich_curated3.where("firstday_ind='Y' or lastday_ind='Y'")


#Grouping & Aggregation
df_agg=txns_cleansed_scrubbed_enrich_curated3_filter.groupBy("category","product").agg(sum("amount").alias("sum_amt"))

#Stopping my Transaction data processing here....
############################################################################################################

#Further we are going to marry the curated cust data (df_munged_enriched_customized_transformed_kpi_format_final)
# with the curated transaction data (txns_cleansed_scrubbed_enrich_curated3)
#Joins, Lookup, Lookup & Enrichment, Denormalization

#APPLICATION OF DIFFERENT TYPES OF JOINS IN OUR REAL LIFE (DBS, DWH, LAKEHOUSES, BIGLAKES, DATALAKES, DELTALAKES)
#-----------------------------------------------------------------------------------------------------------------
spark.conf.set("spark.sql.shuffle.partitions","5")
df_cust=spark.read.csv("file:///home/hduser/sparkdata/custsmodified",inferSchema=True).toDF("custid","fname","lname","age","prof")
df_cust=df_cust.na.drop()
df_cust=df_cust.where("upper(custid)=lower(custid)")
df_txns=txns_cleansed_scrubbed_enrich_curated3
df_txns_2011_12=txns_cleansed_scrubbed_enrich_curated3.where("mth=12")

print("Application of Different Types of JOINS")
print("a. Lookup (Joins)")
#lookup is an activity of identifying some existing data with the help of new data
#Identify how many customer of my overall customers have transacted on December 2011?
#df_cust.count()
#9912

#Ranganayi - semi join (better to use because it will only bring left data, which is need for this scenario)
#Hindupriya - inner join (also produce the same result, but not required to use in the lookup alone scenario)
#Which join is better? semi
cust_count_transacted_on_2011_12_inner=df_cust.join(df_txns_2011_12,on=[col("custid")==col("cid")],how="inner")
cust_count_transacted_on_2011_12_inner=df_cust.join(df_txns_2011_12,on=[col("custid")==col("cid")],how="inner").select("custid").distinct()
cust_count_transacted_on_2011_12_semi=df_cust.join(df_txns_2011_12,on=[col("custid")==col("cid")],how="semi").select("custid").distinct()
cust_count_transacted_on_2011_12_semi.count()
#5475

#Inverse (Anti) Lookup
#Identify how many customer of my overall customers have not transacted on December 2011?
cust_count_transacted_on_2011_12_anti=df_cust.join(df_txns_2011_12,on=[col("custid")==col("cid")],how="anti").select("custid").distinct()
cust_count_transacted_on_2011_12_anti.count()
#4434

#SQL Equivalent with (in or exists) we can use in SQL Databases/DWHs
df_cust.createOrReplaceTempView("custv")
df_txns_2011_12.createOrReplaceTempView("transv")

#semi join equivalent using subquery with in or exists clause
#regular sub query
spark.sql("select count(distinct custid) from custv where custid in (select distinct cid from transv)").show()
#where true/false (custid in (4000001))
#5475
#or
#Corelated subquery (for every custid the subquery will be executed using custid as a filter/join)
spark.sql("select count(distinct custid) from custv where exists (select 'irfan' from transv where cid=custid)").show()
#where exists(true)
#5475

#anti join equivalent using subquery with in or exists clause
#regular sub query
spark.sql("select count(distinct custid) from custv where custid not in (select distinct cid from transv)").show()
#where true/false (custid not in (4000001))
#4434
#or
#Corelated subquery (for every custid the subquery will be executed using custid as a filter/join)
spark.sql("select count(distinct custid) from custv where not exists (select 'irfan' from transv where cid=custid)").show()
#where exists(true)
#4434

#Interview question - Difference between in and exists?
# in will check for the exact values completely
# exists will check for only the first occurance of the given value to search.

print("b. Lookup & Enrichment (Joins)")
#Identify how many customer of my overall customers have transacted on December 2011 and provide me the overall sum(transactions) made?
#Ranganayi - semi join (better to use because it will only bring left data, which cannot be applied for this scenario)
#Hindupriya - inner join (also produce the same result, which can be used for this scenario because we need data from right table also)
#Which join is better?Inner
cust_count_transacted_on_2011_12_inner=df_cust.join(df_txns_2011_12,on=[col("custid")==col("cid")],how="inner").select(sum("amount"))
#cust_count_transacted_on_2011_12_semi=df_cust.join(df_txns_2011_12,on=[col("custid")==col("cid")],how="semi").select("custid").distinct()

#Can we use subquery using in/exists to get the enriched data from the transaction dataset?


print("c. Denormalization - (Schema Modeling) - (Creating Wide Data/Fat Data) - (Star Schema model-Normalized) - "
      "Join Scenario to provide DENORMALIZATION view or flattened or wide tables or fat tables view of data to the business for faster access without doing join")

#Normalized (individual tables, that can be joined on a need basis)/Narrow Tables/Lean Tables/Star schema/Snowflakes schheema tables
#df_cust.write.saveAsTable("cust_dimension")#10 columns
#df_txns.write.saveAsTable("txns_fact")#15 columns

#Denormalized/Widened/Flattened/Fat table (join and provide the joined output in a table)
denormalize_cust_transacted_inner=df_cust.join(df_txns,on=[col("custid")==col("cid")],how="inner")
denormalize_cust_transacted_left=df_cust.join(df_txns,on=[col("custid")==col("cid")],how="left")
denormalize_cust_transacted_left=df_cust.join(df_txns,on=[col("custid")==col("cid")],how="full")
#denormalize_cust_transacted_inner.saveAsTable("cust_trans_wide")
#denormalize_cust_transacted_left.saveAsTable("cust_trans_wide")

#benefits (flattened/widened/denormalized data)
#Once for all we do costly join and flatten the data and store it in the persistant storage (hive/athena/bq/synapse/redshift/parquet)
#Not doing join again and again when consumer does analysis, rather just query the flattened joined data
#drawback(flattened/widened/denormalized data)
#1. duplicate data
#2. occupies storage space

print("Equivalent SQL for Denormalization (denormalization helps to create a single view for faster query execution without joining)")

#Aspirant's SQL Equivalent

print("Application of WINDOWING FUNCTIONALITY")
print("d. Windowing Functionalities")
#clauses/functions - row_number(), rank(), dense_rank() - over(), partition(), orderBy(),Window, desc()
#Windowing functions are very very important functions, used for multiple applications
#Application of Windowing Functions
#Interview Questions:
#Creating Surrogate key/sequence number/identifier fields/primary keys
#3 types of keys to add in any dataset
#1. Natural Key
#2. Primary Key
#3. Surrogate Key
#Identifying & Eliminating Duplicates
#Top N Analysis
#Creating change versions (SCD2)


print("a.How to generate seq or surrogate key column on the ENTIRE data kept in a single partition")
seq_denormalize_cust_transacted_inner=denormalize_cust_transacted_inner.groupBy("prof").agg(avg("age").alias("avg_age")).coalesce(1).\
    select(monotonically_increasing_id().alias("agg_key"),"*")

print("b.How to generate seq or surrogate key column on the ENTIRE data sorted based on the transaction date")
from pyspark.sql.window import Window
#Syntax: row_number().over(Window.orderBy("txndt"))
seq_denormalize_cust_transacted_inner_rno=denormalize_cust_transacted_inner.\
    select(row_number().over(Window.orderBy("txndt")).alias("rno"),"*")

print("c. How to generate seq or surrogate key column accross the CUSTOMERs (partitioned based on the custid) and data sorted based on the transaction date")
#Syntax: row_number().over(Window.partitionBy("custid").orderBy("txndt"))
seq_denormalize_cust_transacted_inner_custid_rno=denormalize_cust_transacted_inner.\
    select(row_number().over(Window.partitionBy("custid").orderBy("txndt")).alias("rno"),"*")

print("d. Least 4 transactions in our overall transactions")
txns_overall_least3=txns_cleansed_scrubbed_enrich_curated3.select(row_number().over(Window.orderBy("amount")).alias("rno"),"*")
txns_overall_least3=txns_overall_least3.where("rno<=4").drop("rno")

print("d. Least 4 (in equal) transactions in our overall transactions")
txns_overall_least3=txns_cleansed_scrubbed_enrich_curated3.select(dense_rank().over(Window.orderBy("amount")).alias("rnk"),"*")
txns_overall_least3=txns_overall_least3.where("rnk<=4").drop("rno")

print("d. Least 3 transactions in our overall transactions")
txns_overall_least3=txns_cleansed_scrubbed_enrich_curated3.select(row_number().over(Window.orderBy("amount")).alias("rno"),"*")
txns_overall_least3=txns_overall_least3.where("rno<=3").drop("rno")

print("e. Top 3 transactions in our overall transactions")
txns_overall_top3=txns_cleansed_scrubbed_enrich_curated3.select(row_number().over(Window.orderBy(desc("amount"))).alias("rno"),"*")
txns_overall_top3=txns_overall_top3.where("rno<=3").drop("rno")

print("f. Top 3 transactions made by the given customer of 4000011,4000012")
top_n_analysis_for_2_customers=txns_cleansed_scrubbed_enrich_curated3.where("cid in (4000011,4000012)")
txns_overall_custid_top3=top_n_analysis_for_2_customers.select(row_number().over(Window.partitionBy("cid").orderBy(desc("amount"))).alias("rno"),"*")
txns_overall_custid_top3.show()
txns_overall_custid_top3=txns_overall_custid_top3.where("rno<=3").drop("rno")
txns_overall_custid_top3.show()

print("g. Top 2nd transaction amount made by the given customer 4000011")
top_n_analysis_for_2_customers=txns_cleansed_scrubbed_enrich_curated3.where("cid in (4000011,4000012)")
txns_overall_custid_top3=top_n_analysis_for_2_customers.select(row_number().over(Window.partitionBy("cid").orderBy(desc("amount"))).alias("rno"),"*")
txns_overall_custid_top3.show()
txns_overall_custid_top3=txns_overall_custid_top3.where("rno=2").drop("rno")
txns_overall_custid_top3.show()

print("i. How to de-duplicate based on certain fields (custid,profession) with priority of least age has to be retained")
df_cust.where("custid =4000001").select(row_number().over(Window.partitionBy("custid","prof").orderBy("age")).alias("rno"),"*")\
    .where("rno=1").drop("rno").show()


print("j. How to de-duplicate based on certain fields eg. Show me whether the given customer have played a category of game =Outdoor Play Equipment atleast once")

top_n_analysis_for_2_customers=txns_cleansed_scrubbed_enrich_curated3.where("category='Outdoor Play Equipment'")
top_n_analysis_for_2_customers.\
    select(row_number().over(Window.partitionBy("cid").orderBy("amount")).alias("rno"),"*")\
    .where("rno=1").drop("rno").show()

#select row_number() over(partition by col1 order by col2)
#select row_number().over(Window.partitionBy(col1).orderBy(col2))

#Identify all the customers who played 'Outdoor Play Equipment' more than 1 time
#Equivalent to use select cid,count(1) from view group by cid having count(1)>1
top_n_analysis_for_2_customers.groupBy("cid").agg(count(lit(1)).alias("cnt")).where("cnt>1").show()


print("E. Analytical Functionalities - LEAD & LAG")
#Interesting Analytical Function called Lead & Lag functions used for hierarchical retrival of data
#Making the data more meaningful by adjusting it... Data Imputation or Data Fabrication
#3. chat - chat initiated 11.21 (actual 11.21) -wrongorder
#4. cc - call center call 11.25 (actual 11.25) - wrongorder
#2. mobile app - purchase 11.35 (actual 11.20) - wrongorder
#1. web - browsing 11.30 (actual 11.10) - wrongorder

#Imputation/Fabrication (more clean to process further)
#1. web - browsing 11.30 (actual 11.10) - wrongorder - 11.21
#2. mobile app - purchase 11.35 (actual 11.20) - wrongorder - 11.25
#3. chat - chat initiated 11.21 (actual 11.21) -wrongorder - 11.30
#4. cc - call center call 11.25 (actual 11.25) - wrongorder - 11.35

print("What is the purchase pattern of the given customer, whether his buying potential is increased or decreased transaction by transaction")

txns_few_custs=txns_cleansed_scrubbed_enrich_curated3.where("cid =4000011")
txns_few_custs_ordered=txns_few_custs.orderBy("txndt")
txns_few_custs_ordered.select("txnno","txndt","amount").show()
'''
+-----+----------+------+
|txnno|     txndt|amount|lag_value
+-----+----------+------+
|73038|2011-01-10| 84.95|0
|13899|2011-03-18|152.01|84.95
|81395|2011-04-20|114.46|152.01
| 2073|2011-05-09| 59.09|
|90023|2011-08-27| 24.47|
|36809|2011-10-20| 104.4|
|35211|2011-11-28|172.74|
+-----+----------+------+
'''

df_lag=txns_few_custs.select("txnno","txndt","amount",lag("amount",offset=1,default=-1).over(Window.orderBy("txndt")).alias("lag_value"))
df_lag_lead=txns_few_custs.select("txnno","txndt","amount",lag("amount",offset=1,default=-1).over(Window.orderBy("txndt")).alias("lag_value"),
                                  lead("amount",offset=1,default=-1).over(Window.orderBy("txndt")).alias("lead_value"))

#We are going to derive 3 columns out of the above lead/lag data
#Is it a first trans
#Is it a last trans
#Customer's purchase pattern or potential
df_first_last_trans=df_lag_lead.withColumn("first_last_trans",when(col("lag_value")==-1,'First Trans').
                                           when(col("lead_value")==-1,'Last Trans'))

df_first_last_trans_pattern=df_first_last_trans.\
    withColumn("purchase_capasity",when(((col("lag_value")==-1) | (col("lead_value")==-1)),col("first_last_trans")).
               when(col("amount")>=col("lag_value"),'Purchase Capasity Increased').
               when(col("amount")<col("lag_value"),'Purchase Capasity Decreased')).drop("first_last_trans")

print("E. Analytical Functionalities - Aggregation/Summarization, Cube, Rollup, Pivot, Statistical Analysis")

df_joined=df_cust.join(df_txns_2011_12,on=[col("custid")==col("cid")],how="inner")
#Aggregated/Summarized (grouping/aggregation and provide the data into output tables)
df_grouped_aggr=df_joined.groupBy("state","city","category","product").\
    agg(sum("amount").alias("sum_amt"),avg("amount").alias("avg_amt"),max("amount").alias("max_amt"),min("amount").alias("min_amt"))

#Cube/Rolled up (aggregation at different levels in the target layer)
#Rollup - Calculate the rolling aggregation value of granular data of the dimensions
#for eg. it will calculate grouping & aggregation based on state,city then state alone then all data
df_grouped_aggr_rollup=df_joined.where("state in ('California','Florida')").rollup("state","city").\
    agg(sum("amount").alias("sum_amt"),avg("amount").alias("avg_amt"),max("amount").alias("max_amt"),min("amount").alias("min_amt"))

#Cube - Cube will help us analyse the data at all possible dimensions (multi dimensional analysis)
#for eg. it will calculate grouping & aggregation based on state,city then state alone then city alone then all data
df_grouped_aggr_cube=df_joined.where("state in ('California','Florida')").cube("state","spendby").\
    agg(sum("amount").alias("sum_amt"),avg("amount").alias("avg_amt"),max("amount").alias("max_amt"),min("amount").alias("min_amt"))

#Pivoted (Expand the column values as rows and provide the data for better analysis)
#Conversion of the row values to the column for better understanding of data by flattening it..

df_grouped_aggr=df_joined.where("state in ('California','Florida')").groupBy("state","city","spendby").agg(sum("amount").alias("sum_amt"))
df_grouped_aggr_pivot=df_joined.where("state in ('California','Florida')").groupBy("state","city").pivot("spendby").agg(sum("amount").alias("sum_amt"))


#EDA Analysis (On top of Enriched, Customized & level1 Curated/Pre Wrangled data)
#Identifying corelation, most frequent occurance,
#Random Sampling
#Random sample of 10% of data to provide to our consumers
df_joined_sample_10pct=df_joined.sample(.1)
df_joined_sample_10pct=df_joined.sample(.2)

#Frequency Analysis
df_freq_age=df_joined.freqItems(["age"])
df_freq_age.show(10,False)

#Corelation Analysis (Know the relation between 2 attributes)
print(df_joined.withColumn("tax_amount",col("amount")+lit(10)).corr("tax_amount","amount"))
print(df_joined.corr("txnno","cid"))

#Statistical Analysis
df_summary=df_joined.summary()

#Trend analysis, Market Basket Analysis, Feature selection, identifying relation between the fields, random sampling, statistical analysis, variance of related attributes...


print("F. Set Operations ")
#Thumb rules : Number, order and datatype of the columns must be same, otherwise unionbyname function you can use.
print("Common customers accross the city, state, stores, products")
df_cust1=df_cust.where("custid in (4000011,4000012,4000013,4000014,4000015)")
df_cust2=df_cust.where("custid in (4000001,4000014,4000015)")

#union (usually in sql db eliminates dups) in pyspark is same
#union all (will have duplicates)
df_cust1.union(df_cust2).show()
#To achive union i have to use distinct
df_cust1.union(df_cust2).distinct().show()

#intersection
df_cust1.intersect(df_cust2).show()

#subract
df_cust1.subtract(df_cust2).show()
df_cust2.subtract(df_cust1).show()#eliminate duplicates also


#How to pass arguments to DSL & SQL?
column1="txnno"
df_first_last_trans_pattern.select(column1)

df_first_last_trans_pattern.createOrReplaceTempView("view1")
spark.sql(f"select {column1} from view1").show()

#Interview Question:
# Subqueries? In DSL we can achive subquery only by using joins (semi/anti)



'''6. Data Persistance (LOAD)-> Data Publishing & Consumption - Enablement of the Cleansed, transformed and analysed data as a Data Product.
#Consumer Data Product
'''

#a. Discovery layer/Consumption/Publishing/Gold Layer we can enable the data
df_joined.write.saveAsTable("joined_txns_custs")

#b. Outbound/Egress
df_grouped_aggr.write.csv("file:///home/hduser/df_grouped_aggr",header=True)

#c. Reports
df_grouped_aggr.write.jdbc(url="jdbc:mysql://127.0.0.1/custpayments",table="df_grouped_aggr",mode="overwrite",
                                        properties={"user":"root","password":"Root123$","driver":"com.mysql.cj.jdbc.Driver",
                                                    "batchsize":"10","numPartitions":"4","queryTimeout":"60","truncate":"true"})

#d. Schema migration
df_summary.write.json("file:///home/hduser/df_summary")

#All We need from here is....
#1. Our Aspirant's are going to create a document about this bb2 program comprise of
#Transformation Terminologies (eg. wrangling, munging),
# Business logics (aggregation, grouping, standardizing),
# Interview scenarios
# Function we used (withcolumn, select)

#2. What's next?
#a. We are going to see the SQL equivalent of the the above DSLs to learn both DSL & SQL relatively
#b. We are going to see the standardized/modernized/Industrialized generic function based BB2 program in a industry standard way..
#c. We package this modernized code and deploy in our cluster (learn about how we can submit spark job with optimized config)

#Finally try to capture and document - the functions, terminologies, interview questions, functionalities -  we used in this entire program
