#PySpark SQL Programming is Platform independent (works anywhere else like hive, bigquery, athena, redshift, synapse...)#
print("***************1. Data Munging *********************")
print("""
1. Data Munging - Process of transforming and mapping data from Raw form into Tidy format with the
intent of making it more appropriate and valuable for a variety of downstream purposes such for analytics/visualizations/analysis/reporting
a. Passive - Data Discovery (EDA) - Exploratory Data Analysis - Performing (Data Exploration) exploratory data analysis of the raw data to identify the attributes(columns/datatype) and patterns (format/sequence/alpha/numeric).
#Understand the data in its own format (read all attributes/columns as string) - total rows, total columns (statistics)
#Understand the data by applying inferschema structure - what is the natural datatypes
#Apply Custom structure using the inferred schema - copy the above columns and datatype into excel and get structure, GenAI (create a structtype with total 100 columns with all as string)   
#Identify Nulls, De-Duplication, Prioritization, Datatypes, formats etc.,
#If the data is not matching with my structure then apply reject strategy
#Summarize the overall pattern of data
b. Active - Data Structurizing - Combining Data + Schema Evolution/Merging/Melting (Structuring) - 
pathlookupfilter,recursivefilelookup, union, unionbyname,allowmissingfields,convert orc/parquet then mergeSchema
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
Duplicates (distinct, dropDuplicates()), Datatypes is not as per the expectation because of the data format hence (rlike or upper()=lower() -> regexp_replace -> cast ) etc.,
Summarize (statistics) the overall trend of data'''

#Define Spark Session Object or sparkContext+sqlContext for writing spark sql program
from learnpyspark.sql.reusable_functions import get_sparksession
spark=get_sparksession("SQL learning application")

spark.conf.set("spark.sql.shuffle.partitions","10")


#a. Data Discovery/understanding/analysis (EDA)
#Understand the data in its own format (read all attributes/columns as string) - total rows, total columns
'''DSL Commented
df_raw=spark.read.csv("file:///home/hduser/sparkdata/custsmodified")
print(df_raw.count())
df_raw.show()
df_raw.printSchema()
'''

'''root
 |-- _c0: string (nullable = true)
 |-- _c1: string (nullable = true)
 |-- _c2: string (nullable = true)
 |-- _c3: string (nullable = true)
 |-- _c4: string (nullable = true)'''


#Understand the data by applying inferschema structure - what is the natural datatypes
'''DSL Commented
df_raw=spark.read.csv("file:///home/hduser/sparkdata/custsmodified",inferSchema=True).toDF("cid","fname","lname","age","prof")
df_raw.show()
df_raw.printSchema()
'''
'''
root
 |-- cid: string (nullable = true)
 |-- fname: string (nullable = true)
 |-- lname: string (nullable = true)
 |-- age: integer (nullable = true)
 |-- prof: string (nullable = true)
'''

#Apply custom schema using infer schema identified columns and datatype, then apply different modes for different data exploration
'''DSL Commented
from pyspark.sql.types import StructType,StructField,StringType,IntegerType
#StructType([StructField("col_name",DataType(),True),StructField("col_name",DataType(),True),StructField("col_name",DataType(),True)......])
customstruct=StructType([StructField("cid",IntegerType(),False),StructField("fname",StringType(),True),StructField("lname",StringType(),True),StructField("age",IntegerType(),True),StructField("prof",StringType(),True)])
df_raw_custom=spark.read.csv("file:///home/hduser/sparkdata/custsmodified",schema=customstruct,mode="permissive")
df_raw_custom.show(20)
df_raw_custom.printSchema()
df_raw_custom.count()#10005
'''

#Let us identify howmuch percent of malformed data is there in our dataset (data is not as per the structure, number of columns are less than the structure defined)
'''DSL Commented
df_raw_custom_malformed=spark.read.csv("file:///home/hduser/sparkdata/custsmodified",schema=customstruct,mode="dropmalformed")
df_raw_custom_malformed.show(20)
df_raw_custom_malformed.printSchema()
df_raw_custom_malformed.count()#10005 dropmalformed will not be applied on count()
len(df_raw_custom_malformed.collect())#10002 - alternative solution is collect and calculate length of the list
'''

#Identify only the corrupted data and I need to analyse/log them or send it to our source system to get it corrected
'''DSL Commented
customstruct_corrupted=StructType([StructField("cid",IntegerType(),False),StructField("fname",StringType(),True),StructField("lname",StringType(),True),StructField("age",IntegerType(),True),StructField("prof",StringType(),True),StructField("corrupted_data",StringType(),True)])
df_raw_custom_malformed=spark.read.csv("file:///home/hduser/sparkdata/custsmodified",schema=customstruct_corrupted,mode="permissive",columnNameOfCorruptRecord="corrupted_data")
df_raw_custom_malformed.show(20)
df_raw_custom_malformed.printSchema()
df_raw_custom_malformed.cache()
df_raw_custom_malformed.where("corrupted_data is not null").show()
df_raw_custom_malformed.where("corrupted_data is not null").show(10,False)#culprit data
'''

#Reject Strategy to store this data in some audit table or audit files and send it to the source system or we do analysis/reprocessing of these data
'''DSL Commented
df_raw_custom_malformed.where("corrupted_data is not null").select("corrupted_data").write.csv("file:///home/hduser/cust_corrupted",mode="overwrite")
'''

#Identify Nulls, constraints, Duplicates, Datatypes, formats etc.,
#Null handling on the key column (null in any, all, subset, constraint)
'''DSL Commented
df_raw_custom_permissive=spark.read.csv("file:///home/hduser/sparkdata/custsmodified",schema=customstruct_corrupted,mode="permissive",columnNameOfCorruptRecord="corrupted_data")
'''

#DSL builtin functions
'''DSL Commented
dropped_col_df=df_raw_custom_permissive.drop("corrupted_data")#drop on the df to drop a column
#We are using "na" which is a pure null handling function
null_any_cols=dropped_col_df.na.drop(how="any")#any one of the column in the given DF has null will be dropped
null_all_cols=dropped_col_df.na.drop(how="all")#all columns in the given DF has null will be dropped

def drop_any(df,h,ss):
    return df.na.drop(how=h,subset=ss)

null_any_cols=dropped_col_df.na.drop(how="any",subset=["cid","prof"])#any one of the column in the given DF has null will be dropped
null_all_cols=dropped_col_df.na.drop(how="all",subset=["cid","prof"])#all columns in the given DF has null will be dropped
print(null_any_cols.count())
print(null_all_cols.count())
'''

#1.Aspirant's Equivalent SQL:
from pyspark.sql.types import StructType,StructField,StringType,IntegerType
customstruct_corrupted=StructType([StructField("cid",IntegerType(),False),StructField("fname",StringType(),True),StructField("lname",StringType(),True),StructField("age",IntegerType(),True),StructField("prof",StringType(),True),StructField("corrupted_data",StringType(),True)])
df_raw_custom_permissive=spark.read.csv("file:///home/hduser/sparkdata/custsmodified",schema=customstruct_corrupted,mode="permissive",columnNameOfCorruptRecord="corrupted_data")

dropped_col_df=df_raw_custom_permissive.drop("corrupted_data")#drop on the df to drop a column

#LETS LEARN HOW TO WRITE SPARK SQL IN MULTIPLE WAYS
###################################################################################################################
#Interview
#Question1: I want to write SQL query rather than DF DSL coding?
#DF (DSL) -> View (SQL)
df_raw_custom_permissive.createOrReplaceTempView("viewname1")
spark.sql(""" select * from viewname1 where cid is not null """).show(2)

#Question2: I want to convert SQL query to DF DSL coding ?
#View (SQL) -> DF (DSL)
df1=spark.sql(""" select * from viewname1 where cid is not null """)
df1.drop("corrupted_data").withColumnRenamed("cid","custid")#DSL is good to rename the column, because we don't have to mention all columns
#DSL Wins here
#or
df1=spark.sql(""" select cid as custid,fname,lname,age,prof from viewname1 where cid is not null """)#If we want to rename we need to select all columns

#Question3: I want to convert SQL query to SQL Query ?
#View (SQL) -> View (SQL)
#deduplicate based on priority using pure SQL
#spark.sql(""" select *,row_number() over(partition by cid order by age desc) rno from viewname1 where rno=1""") # This will not work

#Inline view/from clause subquery
spark.sql(""" 
select * from 
(select *,row_number() over(partition by cid order by age desc) rno from viewname1 ) view2 
where rno=1""")

#We can produce the first tempview output to another tempview and query that final temp view
#viewname1 -> viewname2 -> result

spark.sql(""" create or replace temp view view2
as select *,row_number() over(partition by cid order by age desc) rno from viewname1""")

spark.sql("""select * from view2 where rno=1""").show(2)

#We can produce the first tempview output to another tempview and query that final temp view
#viewname1 -> viewname2 -> result
spark.sql("""select *,row_number() over(partition by cid order by age desc) rno from viewname1""").createOrReplaceTempView("view2")

spark.sql("""select * from view2 where rno=1""").show(2)

#Question4: I want to convert DF DSL to DF DSL ?
df1=df_raw_custom_permissive.where("cid is not null")
df2=df1.drop("corrupted_data").withColumnRenamed("cid","custid")
df2.show(2)

#Question5: Can we write SQL query directly on the files?
df1=spark.read.orc("file:///home/hduser/sparkdata/orcdf1/")
df1.createOrReplaceTempView("orcview1")
spark.sql("select exch from orcview1").show()
#or
spark.sql("""select * from orc.`file:///home/hduser/sparkdata/orcdf1/`""").show()#Direct SQL

#Ebcdic data read
#identify the pyspark ebcdic library
#download and add library in the /usr/local/spark/jars/
#df1=spark.read.format("com.ibm.cobol.ebcdic").load("file:///home/hduser/sparkdata/ebcdic/")

###################################################################################################################
'''DSL Commented
null_any_cols=dropped_col_df.na.drop(how="any")#any one of the column in the given DF has null will be dropped
null_all_cols=dropped_col_df.na.drop(how="all")#all columns in the given DF has null will be dropped

null_any_cols=dropped_col_df.na.drop(how="any",subset=["cid","prof"])#any one of the column in the given DF has null will be dropped
null_all_cols=dropped_col_df.na.drop(how="all",subset=["cid","prof"])#all columns in the given DF has null will be dropped
print(null_any_cols.count())
print(null_all_cols.count())
'''

#Below scenario of using na.drop for huge number of columns DSL is better than SQL
#Below scenario of using na.drop for less number of columns SQL is better than DSL
#any one of the column in the given view has null will be dropped
print("any one of the column in the given view has null will be dropped")
dropped_col_df.createOrReplaceTempView("dropped_col_df")
#null_any_cols=dropped_col_df.na.drop(how="any") #DSL Wins
null_any_cols = spark.sql("""SELECT * FROM dropped_col_df 
WHERE cid IS NOT NULL 
AND fname IS NOT NULL 
AND lname IS NOT NULL 
AND age IS NOT NULL 
AND prof IS NOT NULL""")

print("any one of the column in the given view has null will be SELECTED")
null_any_cols1 = spark.sql("""SELECT * FROM dropped_col_df 
WHERE cid IS NULL 
OR fname IS NULL 
OR lname IS NULL 
OR age IS NULL 
OR prof IS NULL""")

'''
+-------+--------+-----+---+-----+
|    cid|   fname|lname|age| prof|
+-------+--------+-----+---+-----+
|4000001|Kristina|Chung| 55|Pilot|
|4000001|Kristina|Chung| 55|null|
'''

null_any_cols.show(2)

print("all of the column in the given view has null will be dropped")
#null_any_cols=dropped_col_df.na.drop(how="all") #DSL Wins
null_all_cols = spark.sql("""SELECT * FROM dropped_col_df 
WHERE cid IS NOT NULL OR 
fname IS NOT NULL OR 
lname IS NOT NULL OR 
age IS NOT NULL OR 
prof IS NOT NULL""")
null_all_cols.show(2)
#|null|null|null|null|null|
#|1000|null|null|null|null|

print("cid or prof all subset columns with null will be dropped")
#dropped_col_df.na.drop(how="all",subset=["cid","prof"])
#SQL Wins if we use lesser number of columns
null_any_cols_subset = spark.sql("SELECT * FROM dropped_col_df WHERE cid IS NOT NULL OR prof IS NOT NULL")
null_any_cols_subset.show(2)

print("cid of prof any subset columns with null will be dropped")
#dropped_col_df.na.drop(how="any",subset=["cid","prof"])
null_all_cols_subset = spark.sql("SELECT * FROM dropped_col_df WHERE cid IS NOT NULL AND prof IS NOT NULL")
null_all_cols_subset.show(2)

'''DSL Commented

#DSL
df_raw_custom_permissive.where("cid is null").count()#(5 count of column missing and unfit data) permissive converted string to null and customer ids are coming as null literally

df_raw_custom_inferschema=spark.read.csv("file:///home/hduser/sparkdata/custsmodified",inferSchema=True).toDF("cid","fname","lname","age","prof")
#DSL
df_raw_custom_inferschema.where("cid is null").count()#(3 count of only column missing data) how many customer ids are coming as null literally or customer id column is missing (not applicable for csv)

#Null Constraints (Rule I want to putforth on the given column which should not have nulls)
#Sanjay's Question?
#Proactive Interview Question? Also we provided Nullable as False. That will also create an issue right, why spark is not failing? This is open limitation in spark, and we have a workaround for it
#Nullable False represents - if we get null in this column fail our program from further processing
#Workaround - Nullable can't be directly applied on DF, But it can be applied on RDD, and the RDD can be converted back to DF.
#https://issues.apache.org/jira/browse/SPARK-10848
customstruct=StructType([StructField("cid",IntegerType(),False),StructField("fname",StringType(),True),StructField("lname",StringType(),True),StructField("age",IntegerType(),True),StructField("prof",StringType(),True)])
df_raw_custom=spark.read.csv("file:///home/hduser/sparkdata/custsmodified",schema=customstruct,mode="permissive")
#df_raw_custom.show(20)
df_raw_custom.printSchema()
df_raw_custom.count()#10005
#One scope reduction of spark is the not null constraint of nullable false cannot be applied at the time of DF definition, we need to have a workaround
#
df_raw_custom_constrained=df_raw_custom.rdd.toDF(customstruct)
df_raw_custom_constrained=spark.createDataFrame(df_raw_custom.rdd,customstruct)
df_raw_custom_constrained.printSchema()
#Not null constraint is applied now --- ValueError: field cid: This field is not nullable, but got None

#Duplicates - Identification of duplicate records/column values
#Record level duplicates
de_duplicated_rows_df=df_raw_custom.distinct()#DSL/Dataframe functions
#or
df_raw_custom.createOrReplaceTempView("view1")
de_duplicated_rows_df=spark.sql("select distinct * from view1")#SQL (Familiar language)
#or
spark.sql("select cid,fname,lname,age,prof,count(1) from view1 group by cid,fname,lname,age,prof having count(1)>1").show()

#Colum level duplicates
de_duplicated_columns_df=df_raw_custom.dropDuplicates(subset=["cid"])#retain only first record of the duplicates
de_duplicated_columns_df.show()
de_duplicated_columns_df.count()

#If I want to retain only young aged customer
df_raw_custom.sort("age").where("cid=4000003").dropDuplicates(subset=["cid"]).show()

#If I want to retain only old aged customer
df_raw_custom.sort("age",ascending=False).where("cid=4000003").dropDuplicates(subset=["cid"]).show()

#Interview Question: Difference between distinct and dropduplicates in pyspark DSL
#Answer - distinct can be applied on the entire row (all fields), but dropduplicates is for applying in columns level

DSL Commented'''

#Datatypes, formats check on the dataframe
df_raw=spark.read.csv("file:///home/hduser/sparkdata/custsmodified",inferSchema=True).toDF("cid","fname","lname","age","prof")
df_raw.where("rlike(cid,'[a-z|A-z]')=True").show()#DSL Equivalent function
df_raw.createOrReplaceTempView("view1")


#df_dedup_cleansed_scrubbed_c_2.where("rlike(cid,'[a-z|A-z]')=True").show()#no records returned with string data in cid column
#df_dedup_cleansed_scrubbed_c_2.where("rlike(age,'[~-$#@]')=True").show()#no records returned with string data in age column

#rlike, regexp_replace, upper()<>lower()
#If the cid column is alphabet means, then convert to numbers
spark.sql("""select * from view1 where cast(regexp_replace(cid,"[a-z|A-Z]","0") as int)=0""").show()#SQL
spark.sql("""select * from view1 where age<>regexp_replace(age,"[-,~.#!]",'')""").show()#SQL
#handling wrong format (age=7~7) and wrong data type (cid=ten) using regular expression functions
spark.sql("""select cast(regexp_replace(cid,"[a-z|A-Z]","0") as int) cid,fname,lname,
cast(regexp_replace(age,"[-,~.#!]",'') as int)  age,prof 
from view1 """).show()

#2. Aspirant's Equivalent SQL:
#Datatypes, formats check on the dataframe
df_raw=spark.read.csv("file:///home/hduser/sparkdata/custsmodified",inferSchema=True).toDF("cid","fname","lname","age","prof")
df_raw.where("rlike(cid,'[a-z|A-z]')=True").show()#DSL Equivalent function
df_raw.createOrReplaceTempView("view1")

print("cid or age with alpha numeric values")
#rlike, regexp_replace, upper()<>lower()
spark.sql("""SELECT * FROM view1 WHERE rlike(cid, '[a-z|A-Z]') = True""").show()
spark.sql("""SELECT * FROM view1 WHERE cast(regexp_replace(cid, '[a-z|A-Z]', '0') AS int) = 0""").show()
#doing all functionalities in single shot
#df_dedup_cleansed_scrubbed_c_2=df_dedup_cleansed_scrubbed_c_2.withColumn("cid",regexp_replace("cid","[a-z|A-Z]","-2"))
#df_dedup_cleansed_scrubbed_c_2=df_dedup_cleansed_scrubbed_c_2.withColumn("age",regexp_replace("age","[~|-]",""))
#DSL or SQL both wins1
spark.sql("""SELECT 
                cast(regexp_replace(cid, '[a-z|A-Z]', '0') AS int) AS cid,
                fname, 
                lname, 
                cast(regexp_replace(age, '[-,~.#!]', '') AS int) AS age, 
                prof 
             FROM view1""").show()


'''DSL Commented
#Summarize (statistics) the overall trend of data
df_raw.describe().show()
df_raw.summary().show()

DSL Commented'''



#3. Aspirant's Equivalent SQL: df_raw.summary().show()

#DSL Wins here since summary function is readily available,
# if we need customization we can mix with both (DSL+SQL)
#If we need additional metrics on top of summary()
#SQL Wins here since summary function is readily available
#df_raw.summary().createOrReplaceTempView("v1")
#df_raw.createOrReplaceTempView("v2")
#spark.sql("select * from v1 union select select 'avg' as summary,null exch,null sto,avg(rate) rate from v2")

print("Summary metrics of the given view")
spark.sql("""
    SELECT 
        'count' AS summary, 
        COUNT(cid) AS cid, 
        COUNT(fname) AS fname, 
        COUNT(lname) AS lname, 
        COUNT(age) AS age, 
        COUNT(prof) AS prof 
    FROM view1
    UNION ALL
    SELECT 
        'mean', 
        AVG(cid), 
        NULL, 
        NULL, 
        AVG(age), 

        NULL 
    FROM view1
    UNION ALL
    SELECT 
        'stddev', 
        STDDEV(cid), 
        NULL, 
        NULL, 
        STDDEV(age), 
        NULL 
    FROM view1
    UNION ALL
    SELECT 
        'min', 
        MIN(cid), 
        MIN(fname), 
        MIN(lname), 
        MIN(age), 
        MIN(prof) 
    FROM view1
    UNION ALL
    SELECT 
        'max', 
        MAX(cid), 
        MAX(fname), 
        MAX(lname), 
        MAX(age), 
        MAX(prof) 
    FROM view1
    UNION ALL
    SELECT 
        '25%', 
        PERCENTILE_APPROX(cid, 0.25), 
        NULL, 
        NULL, 
        PERCENTILE_APPROX(age, 0.25), 
        NULL 
    FROM view1
    UNION ALL
    SELECT 
        '50%', 
        PERCENTILE_APPROX(cid, 0.50), 
        NULL, 
        NULL, 
        PERCENTILE_APPROX(age, 0.50), 
        NULL 
    FROM view1
    UNION ALL
    SELECT 
        '75%', 
        PERCENTILE_APPROX(cid, 0.75), 
        NULL, 
        NULL, 
        PERCENTILE_APPROX(age, 0.75), 
        NULL 
    FROM view1
""").show()


#If we are reading multiple folders/subfolders/pattern of files then start with DSL then later decide to go with SQL or stay with DSL
'''DSL Commented
#Completed (Passive) a. Data Discovery (every layers ingestion/transformation/consumption) - (Data Governance (security) - Tagging, categorization, classification) (EDA) (Data Exploration) - Performing an (Data Exploration) exploratory data analysis of the raw data to identify the attributes and patterns.

print("Active - b. Combining Data + Schema Evolution/Merging (Structuring)")
[hduser@localhost sparkdata]$ cp -R custdata custdata1
[hduser@localhost sparkdata]$ mkdir custdata/dir1
[hduser@localhost sparkdata]$ mkdir custdata/dir2
[hduser@localhost sparkdata]$ cd custdata
[hduser@localhost custdata]$ cp custsmodified1 dir1/custsmodified4
[hduser@localhost custdata]$ cp custsmodified1 dir1/custsmodified5
[hduser@localhost custdata]$ cp custsmodified1 dir2/custsmodified6
[hduser@localhost custdata]$ cp custsmodified1 dir2/custsmodified7

print("b.1. Combining Data - Reading from a path contains multiple pattern of files")
#Thumb rule/Caveat is to have the data in the expected structure with same number and order of columns
df_raw=spark.read.csv(path="file:///home/hduser/sparkdata/custdata/",inferSchema=True,pathGlobFilter="custsmodified[1-9]",header=True)

print("b.2. Combining Data - Reading from a multiple different paths contains multiple pattern of files")
df_raw=spark.read.csv(path=["file:///home/hduser/sparkdata/custdata/","file:///home/hduser/sparkdata/custdata1/"],inferSchema=True,pathGlobFilter="custsmodified[1-9]",header=True)

print("b.2. Combining Data - Reading from a multiple sub paths contains multiple pattern of files")
df_raw=spark.read.csv(["file:///home/hduser/sparkdata/custdata/","file:///home/hduser/sparkdata/custdata1/"],inferSchema=True,recursiveFileLookup=True,pathGlobFilter="custsmodified[1-9]",header=True)

print("b.3. Schema Merging (Structuring) leads to Schema Evolution - Schema Merging data with different structures (we know the structure of both datasets)")
#When I want to extract data in a proper structure with the right column mapping
df_raw1=spark.read.csv("file:///home/hduser/sparkdata/custdata/custsmodified1",inferSchema=True,header=True)#We are not sure about the varying structure, hence going with inferSchema
df_raw2=spark.read.csv("file:///home/hduser/sparkdata/custdata/custsmodified2",inferSchema=True,header=True)
df_raw3=spark.read.csv("file:///home/hduser/sparkdata/custdata/custsmodified3",inferSchema=True,header=True)

#Let's try with union
from pyspark.sql.functions import *
#union works only if we have same structure of data with same number of columns and columns will be taken in positional notation
df_raw1.union(df_raw2).show()#This union will not work since raw1 contains 5 columns and raw2 contains 6 columns

#We need manual intervention to handle this schema merging
df_union1=df_raw1.select("*",lit("null").alias("city")).union(df_raw2)
df_unionall_by_aspirants=df_union1.union(df_raw3.select("custid","fname","lname",lit(0).alias("age"),"prof","city"))
df_unionall_by_aspirants.show()

#special spark DSL function unionByName with allowMissingColumns feature
df_unionall_byspark=df_raw1.unionByName(df_raw2,allowMissingColumns=True).unionByName(df_raw3,allowMissingColumns=True)

DSL commented'''

#4. Aspirant's Equivalent SQL: Need the data in this format df_unionall_byspark or df_unionall_by_aspirants
df_raw1=spark.read.csv("file:///home/hduser/sparkdata/custdata/custsmodified1",inferSchema=True,header=True)
df_raw2=spark.read.csv("file:///home/hduser/sparkdata/custdata/custsmodified2",inferSchema=True,header=True)
df_raw3=spark.read.csv("file:///home/hduser/sparkdata/custdata/custsmodified3",inferSchema=True,header=True)

df_raw1.createOrReplaceTempView("df_raw1")
df_raw2.createOrReplaceTempView("df_raw2")
df_raw3.createOrReplaceTempView("df_raw3")

#DSL wins here - Since unionByName function will take care of schema merging dynamically, this optio is not available in SQL
# Select all columns from df_raw1 and add a placeholder for the missing 'city' column
print("Select all columns from df_raw1 and add a placeholder for the missing 'city' column")

spark.sql("""
    SELECT custid, fname, lname, age, prof, 'null' AS city FROM df_raw1
    UNION ALL
    SELECT custid, fname, lname, age, prof, city FROM df_raw2
""").createOrReplaceTempView("df_union12")

# Select and align columns from df_raw3, ensuring all columns are present
print("Select and align columns from df_raw3, ensuring all columns are present")
spark.sql("""
    SELECT custid, fname, lname, age, prof, city FROM df_union12
    UNION ALL
    SELECT custid, fname, lname, 0 AS age, prof, city FROM df_raw3
""").show()

#To union all views in one shot with deduplication
spark.sql("""
    select distinct * from (
    SELECT distinct custid, fname, lname, age, prof, null AS city FROM df_raw1
    UNION ALL
    SELECT distinct custid, fname, lname, age, prof, city FROM df_raw2
    UNION ALL    
    SELECT distinct custid, fname, lname, 0 AS age, prof, city FROM df_raw3) t
""").show()#Completely deduplicated result

#No equivalent SQL available for unionByName with allowmissingcolumns
print("No equivalent SQL available for unionByName with allowmissingcolumns")

'''DSL Commented
df_unionall_byspark = df_raw1.unionByName(df_raw2, allowMissingColumns=True).unionByName(df_raw3, allowMissingColumns=True)
df_unionall_byspark.show()


print("b.3. Schema Evolution (Structuring) - source data is evolving with different structure")
df_raw1=spark.read.csv("file:///home/hduser/sparkdata/custdata/custsmodified1",inferSchema=True,header=True)#We are not sure about the varying structure, hence going with inferSchema
df_raw2=spark.read.csv("file:///home/hduser/sparkdata/custdata/custsmodified2",inferSchema=True,header=True)
df_raw3=spark.read.csv("file:///home/hduser/sparkdata/custdata/custsmodified3",inferSchema=True,header=True)

df_raw3_modified=df_raw3.withColumn("custid",col("custid").cast("string"))#DSL to convert custid into string type
df_raw3_modified.printSchema()
#sql to conver custid into string type
df_raw3.createOrReplaceTempView("view1")
df_raw3_modifiedsql=spark.sql("select cast(custid as string) custid,lname,fname,prof,city from view1")
df_raw3_modifiedsql.printSchema()

#Schema merging can leads to schema Evolution also
df_raw1.write.orc("file:///home/hduser/df_raworc1")
df_raw2.write.orc("file:///home/hduser/df_raworc2")
df_raw3_modified.write.orc("file:///home/hduser/df_raworc3",mode="overwrite")

df_mergeSchema_byspark=spark.read.orc(path=["file:///home/hduser/df_raworc1","file:///home/hduser/df_raworc2","file:///home/hduser/df_raworc3"],mergeSchema=True)
#One challenge in the above scenario is the columns with same name should be of same type
df_mergeSchema_byspark.show(100)#merged and evolved schema dataframe

print("c.1. Data Preparation/Preprocessing - Validation (active)- DeDuplication, Prioritization - distinct, dropDuplicates, windowing (rank/denserank/rownumber), groupby having")

df_raw=spark.read.csv("file:///home/hduser/sparkdata/custsmodified",inferSchema=True).toDF("cid","fname","lname","age","prof")
df_row_dedup=df_raw.distinct()#row level
df_column_dedup=df_row_dedup.dropDuplicates(["cid"])#columns level (custid occuring first will be retained, rest of the same custid will be dropped)
df_column_priority_dedup_sorted_c_1=df_raw.sort("age",ascending=False).dropDuplicates(["cid"]).sort("cid")#drop the young aged customers and retain old age customer
#where("cid =4000003").
#important and we learn more in detail about this later
#Window-> row_number(over(partitionBy().orderBy()))
from pyspark.sql.window import Window
df_column_priority_dedup=df_row_dedup.where("cid =4000003").\
    select("*",row_number().over(Window.partitionBy("cid").orderBy(col("age").desc()))
    .alias("rownum")).where("rownum=1").\
    drop("rownum")#drop the young aged customers and retain old age customer

df_column_priority_dedup.show()

DSL Commented '''

df_raw=spark.read.csv("file:///home/hduser/sparkdata/custsmodified",inferSchema=True).toDF("cid","fname","lname","age","prof")
#5. Aspirant's Equivalent SQL: try mixing sql and dsl
#df_row_dedup-> tempview->select * from (select row_number(over(partition by cid order by age desc)) as rownum from view) temp where...
df_raw.createOrReplaceTempView("view1")

#Row Level Deduplication:
#df_row_dedup=df_raw.distinct()#row level
spark.sql("""
    CREATE OR REPLACE TEMP VIEW df_row_dedup AS
    SELECT DISTINCT * FROM view1
""")
#or
'''spark.sql("""
        SELECT DISTINCT * FROM view1
""").createOrReplaceTempView("df_row_dedup")
'''

print("Row Level Deduplication:",spark.sql("select * from df_row_dedup").count())

#Column Level Deduplication:

'''from pyspark.sql.window import Window
df_column_priority_dedup=df_row_dedup.where("cid =4000003").\
    select("*",row_number().over(Window.partitionBy("cid").orderBy(col("age").desc()))
    .alias("rownum")).where("rownum=1").\
    drop("rownum")#drop the young aged customers and retain old age customer
'''

#Prioritize by Age and Retain Older Customers:
#sql syntax for windowing function row_number() over(partition by cid order by age desc)
spark.sql("""
    CREATE OR REPLACE TEMP VIEW df_column_priority_dedup_sorted_c_1 AS
    SELECT * FROM (
        SELECT *, row_number() OVER(PARTITION BY cid ORDER BY age DESC) as row_num
        FROM view1
    ) tmp
    WHERE row_num = 1
    ORDER BY cid
""")

print("Prioritize by Age and Retain Older Customers:",spark.sql("select * from df_column_priority_dedup_sorted_c_1").count())


#Use Window Function to Prioritize for Specific cid:
spark.sql("""
    CREATE OR REPLACE TEMP VIEW df_column_priority_dedup AS
    SELECT * FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY cid ORDER BY age DESC) as rownum
        FROM df_row_dedup
        WHERE cid = 4000003
    ) tmp
    WHERE rownum = 1
""")
print("Use Window Function to Prioritize for Specific cid:",spark.sql("select * from df_column_priority_dedup").count())
spark.sql("select * from df_column_priority_dedup_sorted_c_1 where cid=4000003").show()


'''DSL Commented
#Very very importand interview question
#how to identify/delete dup data from a table
#how to identify nth max/min sal/sales/age from the data

print("c.2. Active - Data Preparation/Preprocessing/Validation (Cleansing & Scrubbing) - Identifying and filling gaps & Cleaning data to remove outliers and inaccuracies")
#We are going to use na function to achieve both cleansing and scrubbing
#Cleansing - uncleaned vessel will be thrown (na.drop is used for cleansing)
#df_raw=spark.read.csv("file:///home/hduser/sparkdata/custsmodified_na",inferSchema=True,header=True).toDF("cid","lname","fname","age","prof")
df_column_priority_dedup_sorted_c_1.na.drop()#drop is used to remove the rows with null column(s) values (default how=any)

df_cleansed_c_2=df_column_priority_dedup_sorted_c_1.na.drop(how="any")#Default is any (any column in the given row contains null will be removed
df_all_col_null=df_column_priority_dedup_sorted_c_1.na.drop(how="all")#all (all column in the given row contains null will be removed)

df_any_col_null=df_raw.na.drop(how="any",subset=["fname","prof"])#any one column fname and prof has null will be dropped - for learning we are using df_raw
df_all_col_null=df_raw.na.drop(how="all",subset=["fname","prof"])#both fname and prof has null will be dropped - for learning we are using df_raw

df_thresh_col_null=df_raw.na.drop(thresh=2)#Thresh is used to control the threshold of how many columns with not null I am expecting in the dataframe
df_thresh_col_null=df_raw.na.drop(thresh=2,subset=["cid","fname","prof"])#Thresh is used to control the threshold of how many columns with not null I am expecting in the dataframe

DSL Commented'''

#6. Aspirant's Equivalent SQL:

df_raw.createOrReplaceTempView("df_raw")

#Drop Rows with Any column with Null Values:

spark.sql("""
    CREATE OR REPLACE TEMP VIEW df_cleansed_c_2 AS
    SELECT * FROM df_column_priority_dedup_sorted_c_1
    WHERE not(cid IS NULL OR fname IS NULL OR lname IS NULL OR age IS NULL OR prof IS NULL)
    --WHERE cid IS not NULL and fname IS not NULL OR lname IS not NULL OR age IS not NULL OR prof IS not NULL)
""")

print("Drop Rows with Any Null Values:",spark.sql("select * from df_cleansed_c_2").count())

#Drop Rows Where All columns Are Null:
spark.sql("""
    CREATE OR REPLACE TEMP VIEW df_all_col_null AS
    SELECT * FROM df_column_priority_dedup_sorted_c_1
    WHERE NOT(cid IS NULL AND fname IS NULL AND lname IS NULL AND age IS NULL AND prof IS NULL)
""")
print("Drop Rows Where All Values Are Null using negation:",spark.sql("select * from df_all_col_null").count())

#Drop Rows Where Any Value in fname or prof is Null:
spark.sql("""
    CREATE OR REPLACE TEMP VIEW df_any_col_null AS
    SELECT * FROM df_raw
    WHERE NOT (fname IS NULL OR prof IS NULL)
""")

print("Drop Rows Where Any Value in fname or prof is Null:",spark.sql("select * from df_any_col_null").count())

#Drop Rows Where both fname and prof Are Null:
spark.sql("""
    CREATE OR REPLACE TEMP VIEW df_all_col_null AS
    SELECT * FROM df_raw
    WHERE NOT (fname IS NULL AND prof IS NULL)
""")
print("Drop Rows Where Any Value in fname or prof is Null:",spark.sql("select * from df_all_col_null").count())
spark.sql("select * from df_all_col_null").show(2)

#df_thresh_col_null=df_raw.na.drop(thresh=2)#thresh is for identifying not null columns
#DSL Wins literally
#if we need to identify null columns with some minimum count then we can go with SQL
#Drop Rows with Less Than 2 Non-Null Values: cid,fname,lname,age,prof - if cid has value, age has value then select that row
#Validation of the logic we write is right/wrong?
spark.sql("""select *, 
case when cid is not null then 1 else 0 end+ 
case when fname is not null then 1 else 0 end +
case when lname is not null then 1 else 0 end + 
case when age is not null then 1 else 0 end + 
case when prof is not null then 1 else 0 end AS num_cols_with_not_null 
from view1 """).show(20)

spark.sql("""
    CREATE OR REPLACE TEMP VIEW df_thresh_col_null AS
    SELECT * FROM df_raw
    WHERE 
        CASE WHEN cid IS NOT NULL THEN 1 ELSE 0 END + 
        CASE WHEN fname IS NOT NULL THEN 1 ELSE 0 END + 
        CASE WHEN lname IS NOT NULL THEN 1 ELSE 0 END + 
        CASE WHEN age IS NOT NULL THEN 1 ELSE 0 END + 
        CASE WHEN prof IS NOT NULL THEN 1 ELSE 0 END >= 2
""")
print("Drop Rows with Less Than 2 Non-Null Values:",spark.sql("select * from df_thresh_col_null").count())
spark.sql("select * from df_thresh_col_null").show(2)

#Drop Rows with Less Than 2 Non-Null Values in cid, fname, and prof:
spark.sql("""
    CREATE OR REPLACE TEMP VIEW df_thresh_col_null AS
    SELECT * FROM df_raw
    WHERE 
        CASE WHEN cid IS NOT NULL THEN 1 ELSE 0 END + 
        CASE WHEN fname IS NOT NULL THEN 1 ELSE 0 END + 
        CASE WHEN prof IS NOT NULL THEN 1 ELSE 0 END >= 2
""")

print("Drop Rows with Less Than 2 Non-Null Values in cid, fname, and prof:",spark.sql("select * from df_thresh_col_null").count())
spark.sql("select * from df_thresh_col_null").show(2)


'''DSL Commented
#Scrubbing (repairing) - repair and reusable vessel (na.fill and na.replace is used for scrubbing or filling gaps)
df_scrubbed_learning=df_raw.na.fill("NA",subset=["fname","lname","prof"])#Fill will help us fill the null values with some values, rather than dropping null, fill it
df_scrubbed_learning=df_raw.na.fill("-1",subset=["cid","age"])

prof_dict={"Therapist":"Physio Therapist","Doctor":"Physician"}#SQL Equivalent case statement
df_scrubbed_learning=df_raw.na.replace(prof_dict,subset=["prof"])#Replace the dictory pattern in the given column of the dataset


df_dedup_cleansed_scrubbed_c_2=df_cleansed_c_2.na.fill("NA",subset=["fname","lname","prof"]).na.fill("-1",subset=["cid","age"])

prof_dict={"Therapist":"Physio Therapist","Doctor":"Physician"}#SQL Equivalent case statement
df_dedup_cleansed_scrubbed_c_2=df_cleansed_c_2.na.replace(prof_dict,subset=["prof"])#Replace the dictory pattern in the given column of the dataset

#EDA - Finding any cid is having string value in it using rlike function
df_dedup_cleansed_scrubbed_c_2.where("rlike(cid,'[a-z|A-z]')=True").show()

#Replace the string with some number -1 or age correction respectively on cid, age column
df_dedup_cleansed_scrubbed_c_2=df_dedup_cleansed_scrubbed_c_2.withColumn("cid",regexp_replace("cid","[a-z|A-Z]","-2"))
df_dedup_cleansed_scrubbed_c_2=df_dedup_cleansed_scrubbed_c_2.withColumn("age",regexp_replace("age","[~|-]",""))
df_dedup_cleansed_scrubbed_c_2.where("rlike(cid,'[a-z|A-z]')=True").show()#no records returned with string data in cid column
df_dedup_cleansed_scrubbed_c_2.where("rlike(age,'[~-$#@]')=True").show()#no records returned with string data in age column

DSL Commented'''

#7. Aspirant's Equivalent SQL:
#df_raw.createOrReplaceTempView("df_raw")
#df_cleansed_c_2.createOrReplaceTempView("df_cleansed_c_2")
#df_dedup_cleansed_scrubbed_c_2=df_cleansed_c_2.na.fill("NA",subset=["fname","lname","prof"])
# .na.fill("-1",subset=["cid","age"])
#DSL wins as it is simple to achieve if we want to find nulls and fill with something else
#Fill Null Values in Specific Columns:
spark.sql("""
    CREATE OR REPLACE TEMP VIEW df_scrubbed_learning AS
    SELECT 
        COALESCE(fname, 'NA') AS fname,
        COALESCE(lname, 'NA') AS lname,
        COALESCE(prof, 'NA') AS prof,
        COALESCE(CAST(cid AS STRING), '-1') AS cid,
        COALESCE(CAST(age AS STRING), '-1') AS age
    FROM df_raw
""")

print("Fill Null Values in Specific Columns:",spark.sql("select * from df_scrubbed_learning").count())
spark.sql("select * from df_scrubbed_learning").show(2)

#SQL wins if we want to find not only nulls, some other values also like blank space
#Just to write some dummy queries for testing something...
spark.sql("""select fname, CASE WHEN COALESCE(fname,'')='' then 'NA' else fname end AS fname1 
from (select null as fname union all select '' as fname union all select 'irfan' as fname)dummyview1
""").show()

spark.sql("""
    CREATE OR REPLACE TEMP VIEW df_scrubbed_learning AS
    SELECT 
        CASE WHEN COALESCE(fname,'')='' then 'NA' else fname end AS fname,
        COALESCE(lname, 'NA') AS lname,
        COALESCE(prof, 'NA') AS prof,
        COALESCE(CAST(cid AS STRING), '-1') AS cid,
        COALESCE(CAST(age AS STRING), '-1') AS age
    FROM df_raw
""")

#Replace Specific Values in a Column:
#prof_dict={"Therapist":"Physio Therapist","Doctor":"Physician"}#SQL Equivalent case statement
#df_dedup_cleansed_scrubbed_c_2=df_cleansed_c_2.na.replace(prof_dict,subset=["prof"])
#DSL Wins because of direct function to achieve
spark.sql("""
    CREATE OR REPLACE TEMP VIEW df_scrubbed_learning2 AS
    SELECT 
        cid, fname, lname, age,
        CASE 
            WHEN prof = 'Therapist' THEN 'Physio Therapist' 
            WHEN prof = 'Doctor' THEN 'Physician'
            ELSE prof
        END AS prof
    FROM df_scrubbed_learning
""")

print("Replace Specific Values in a Column:",spark.sql("select * from df_scrubbed_learning2").count())
spark.sql("select * from df_scrubbed_learning2").show(2)

#SQL Wins because of some achieving the output dynamically
# (Eg: wanted to check the given year in the date column is matching with current year)
#Requirement is - if the column considering is having the first 3 values is The or Doc
# then change it to Physio Therapist or Physician accordingly
spark.sql("""
    CREATE OR REPLACE TEMP VIEW df_scrubbed_learning2 AS
    SELECT 
        cid, fname, lname, age,
        CASE 
            WHEN UPPER(substr(prof,1,3)) = 'THE' THEN 'Physio Therapist' 
            WHEN UPPER(substr(prof,1,3)) = 'DOC' THEN 'Physician'
            ELSE prof
        END AS prof
    FROM df_scrubbed_learning
""")

#Combining Fill and Replace Operations (Scrubbing):
spark.sql("""
    CREATE OR REPLACE TEMP VIEW df_dedup_cleansed_scrubbed_c_2 AS
    SELECT 
        COALESCE(fname, 'NA') AS fname,
        COALESCE(lname, 'NA') AS lname,
        CASE 
            WHEN prof = 'Therapist' THEN 'Physio Therapist' 
            WHEN prof = 'Doctor' THEN 'Physician'
            ELSE COALESCE(prof, 'NA')
        END AS prof,
        COALESCE(CAST(cid AS STRING), '-1') AS cid,
        COALESCE(CAST(age AS STRING), '-1') AS age
    FROM df_cleansed_c_2
""")

print("Combining Fill and Replace Operations:",spark.sql("select * from df_dedup_cleansed_scrubbed_c_2").count())
spark.sql("select * from df_dedup_cleansed_scrubbed_c_2").show(2)


#Detecting String Values in cid Column Using rlike:
#Both SQL and DSL wins here
print("Detecting String Values in cid Column Using rlike:")
spark.sql("""
    SELECT * FROM df_dedup_cleansed_scrubbed_c_2
    WHERE cid RLIKE '[a-zA-Z]'
""").show(2)

#Replacing String Values with Numbers in cid and Cleaning age Column:
print("Replacing String Values with Numbers in cid and Cleaning age Column:")
spark.sql("""
    CREATE OR REPLACE TEMP VIEW df_dedup_cleansed_scrubbed_c_2_a AS
    SELECT 
        REGEXP_REPLACE(cid, '[a-zA-Z]', '-2') AS cid,
        REGEXP_REPLACE(age, '[~|-]', '') AS age,
        fname, lname, prof
    FROM df_dedup_cleansed_scrubbed_c_2
""")

#Verifying No String Values in cid and age Columns:
print("Verifying/Unit Testing No String Values in cid and age Columns:")
spark.sql("""
    SELECT * FROM df_dedup_cleansed_scrubbed_c_2_a
    WHERE cid RLIKE '[a-zA-Z]'
""").show(2)

spark.sql("""
    SELECT * FROM df_dedup_cleansed_scrubbed_c_2_a
    WHERE age RLIKE '[~|-|$|#|@]'
""").show(2)


print("d.1. Data/Attributes(name/type) Standardization (column) - Column re-order/number of columns changes (add/remove/Replacement/Renaming)  to make it in a understandable/usable format")
'''DSL Commented
#Apply few column functions to do column - reordering, rename, removal, replacement, add, changing type of the columns
#To make this data more meaningful we have to add the sourcesystem from where we received the data from - withColumn
#To make this data more meaningful we have to rename the columns like cid as custid, prof as profession - withColumnRenamed/alias
#To make this data more meaningful we have to replace the columns like fname with lname from the dataframe - withColumn
#To make this data more meaningful we have to remove the columns like lname from the dataframe - drop
#To make this data more meaningful we have to typecast like custid as int and age as int - cast
#To make this data more meaningful we have to reorder the columns like custid,fname,profession,age - select/withColumn

#How to use important DSL column management functions like -
# withColumn("new_colname",Column(lit/col/functionoutput())) - add/replace/reorder
# withColumnRenamed("existing_column","new_column_name")
# drop
from pyspark.sql.functions import *
#Standardization 1 - Ensure to add source system for all our data
df_dedup_cleansed_scrubbed_standardized_d_1=df_dedup_cleansed_scrubbed_c_2.withColumn("sourcesystem",lit("Retail Stores"))

#Standardization 2 - Ensure to rename the columns with the abbrevation extended
df_dedup_cleansed_scrubbed_standardized_d_1=df_dedup_cleansed_scrubbed_standardized_d_1.withColumnRenamed("cid","custid").withColumnRenamed("prof","profession")

#Doing some analysis on the dataframe to check whether we have 3 names in fullname
df_dedup_cleansed_scrubbed_standardized_d_1=df_dedup_cleansed_scrubbed_standardized_d_1.withColumn("fullname",concat("fname",lit(" "),"lname"))
df_dedup_cleansed_scrubbed_standardized_d_1.where(size(split("fullname"," "))>2).show()

#Remove columns - removing post analysis columns or some source columns that is not needed
#Standardization 3 - Ensure to remove the columns that we found unwanted to propogate further
df_dedup_cleansed_scrubbed_standardized_d_1=df_dedup_cleansed_scrubbed_standardized_d_1.drop("fullname")

#replace profession with upper(profession)
#Standardization 4 - Ensure to have the descriptive columns like profession, category, city, state of upper case
df_dedup_cleansed_scrubbed_standardized_d_1=df_dedup_cleansed_scrubbed_standardized_d_1.withColumn("profession",upper("profession"))

#Typecasting - Changing the type of the column appropriately - cid int, age int
#Standardization 5 - Ensure to redefine the dataframe with the appropriate datatypes
df_dedup_cleansed_scrubbed_standardized_d_1.printSchema()
df_dedup_cleansed_scrubbed_standardized_d_1=df_dedup_cleansed_scrubbed_standardized_d_1.withColumn("custid",col("custid").cast("int")).\
withColumn("age",col("age").cast("int"))

#Reordering of the column custid,fname,profession,age
df_dedup_cleansed_scrubbed_standardized_d_1=df_dedup_cleansed_scrubbed_standardized_d_1.select("custid","fname","lname","profession","age","sourcesystem")

DSL Commented'''

#8. Aspirant's SQL Equivalent
#df_dedup_cleansed_scrubbed_c_2.createOrReplaceTempView("df_dedup_cleansed_scrubbed_c_2")

#SQL Wins here
#Standardization 1 - Add Source System Column:
print("Standardization 1 - Add Source System Column:")
spark.sql("""
    CREATE OR REPLACE TEMP VIEW df_standardized AS
    SELECT *, 'Retail Stores' AS sourcesystem
    FROM df_dedup_cleansed_scrubbed_c_2
""")

#DSL wins because I can just say withColumnRenamed to rename a given column without select all the other columns
#Standardization 2 - Ensure to rename the columns with the abbrevation extended
print("Standardization 2 - Ensure to rename the columns with the abbrevation extended")
spark.sql("""
    CREATE OR REPLACE TEMP VIEW df_standardized2 AS
    SELECT 
        cid AS custid, 
        fname, 
        lname, 
        prof AS profession, 
        age, 
        sourcesystem
    FROM df_standardized
""")

#SQL wins because of familiarity
#Doing some analysis on the dataframe to check whether we have 3 names in fullname
print("Doing some analysis on the dataframe to check whether we have 3 names in fullname")
spark.sql("""
    CREATE OR REPLACE TEMP VIEW df_with_fullname AS
    SELECT *, CONCAT(fname, ' ', lname) AS fullname
    FROM df_standardized2
""")

#SQL wins because of familiarity
print("Final Standardized 1 and 2 Output")
spark.sql("""
    SELECT * FROM df_with_fullname
    WHERE SIZE(SPLIT(fullname, ' ')) > 2
""").show()


print("Standardization 3 - Ensure to remove the columns that we found unwanted to propogate further")
#Remove columns - removing post analysis columns or some source columns that is not needed
#DSL wins here because rather than selecting all columns that is needed, we can just drop the column which is not needed
#Standardization 3 - Ensure to remove the columns that we found unwanted to propogate further
spark.sql("""
    CREATE OR REPLACE TEMP VIEW df_standardized3 AS
    SELECT custid, fname, lname, profession, age, sourcesystem
    FROM df_with_fullname
""")

print("Standardization 4 - Ensure to have the descriptive columns like profession of upper case")
#SQL wins due to familiarity
#replace profession with upper(profession)
#Standardization 4 - Ensure to have the descriptive columns like profession, category, city, state of upper case
spark.sql("""
    CREATE OR REPLACE TEMP VIEW df_standardized4 AS
    SELECT custid, fname, lname, UPPER(profession) AS profession, age, sourcesystem
    FROM df_standardized3

""")

print("Standardization 5 - Ensure to redefine the dataframe with the appropriate datatypes")
#Typecasting - Changing the type of the column appropriately - cid int, age int
#SQL wins due to familiarity
#Standardization 5 - Ensure to redefine the dataframe with the appropriate datatypes
spark.sql("""
    CREATE OR REPLACE TEMP VIEW df_standardized5 AS
    SELECT 
        CAST(custid AS INT) AS custid, 
        fname, 
        lname, 
        profession, 
        CAST(age AS INT) AS age, 
        sourcesystem
    FROM df_standardized4
""")

print("Reordering of the column custid,fname,profession,age")
#Reordering of the column custid,fname,profession,age
#SQL wins due to familiarity
print("Final Standardized Output")
spark.sql("""
    SELECT 
        custid, 
        fname, 
        lname, 
        profession, 
        age, 
        sourcesystem
    FROM df_standardized5
""").show()

print("********************1. data munging completed****************")


'''2. Starting - Data Enrichment - Makes your data rich and detailed
a. Add, Remove, Rename, Modify/replace
b. split, merge/Concat
c. Type Casting, format & Schema Migration
'''

print("*************** Data Enrichment (values)-> Add, Rename, merge(Concat), Split, Casting of Fields, Reformat, "
      "replacement of (values in the columns) - Makes your data rich and detailed *********************")
'''DSL Commented
#Add Columns - for this pipeline, I needed the load date and the timestamp
df_munged_enriched1=df_dedup_cleansed_scrubbed_standardized_d_1.withColumn("loaddt",lit("2024-05-23")).withColumn("loadts",current_timestamp())

#Rename Columns
df_munged_enriched2=df_munged_enriched1.withColumnRenamed("fname","firstname").withColumnRenamed("lname","lastname")

#Swapping Columns
#By renaming and reordering
df_munged_enriched4=df_munged_enriched2.select("custid",col("lastname").alias("firstname"),col("firstname").alias("firstnametmp"),"profession","age","sourcesystem","loaddt","loadts").withColumnRenamed("firstnametmp","lastname")

#By renaming and reordering
df_munged_enriched3=df_munged_enriched2.withColumnRenamed("lastname","firstname1").withColumnRenamed("firstname","lastname").withColumnRenamed("firstname1","firstname")
df_munged_enriched4=df_munged_enriched3.select("custid","firstname","lastname","profession","age","sourcesystem","loaddt","loadts")

#By introducing temp column and dropping it
df_munged_enriched3=df_munged_enriched2.withColumn("lastnametmp",col("lastname")).drop("lastname").withColumnRenamed("firstname","lastname").withColumnRenamed("lastnametmp","firstname")
df_munged_enriched4=df_munged_enriched3.select("custid","firstname","lastname","profession","age","sourcesystem","loaddt","loadts")

#Replace Columns (just for learning)
learn_df=df_munged_enriched2.withColumn("firstname",col("lastname"))#data in fname will be replaced with lname

#b. merge/Concat, split
#merge
df_munged_enriched5=df_munged_enriched4.withColumn("mailid",concat("firstname",lit("."),"lastname",lit("."),"custid",lit("@inceptez.com")))

#Split
df_munged_enriched6=df_munged_enriched5.withColumn("splitcol",split("mailid","@")).withColumn("userid",col("splitcol")[0]).withColumn("domain",col("splitcol")[1])
#try do the above with select

#Remove Columns
df_munged_enriched7=df_munged_enriched6.drop("splitcol")

#c. Type Casting, format & Schema Migration
#Type casting
#We need to change the loaddt into date format
df_munged_enriched8=df_munged_enriched7.select("*",col("loaddt").cast("date").alias("loaddt"))#usage of select with all columns typed is not good here since we require dropping of loaddt1 and reordering
#or
df_munged_enriched8=df_munged_enriched7.withColumn("loaddt",col("loaddt").cast("date"))#best option to choose with huge efforts

#Reformatting & Extraction
#Extraction
df_munged_enriched9=df_munged_enriched8.withColumn("year",year(col("loaddt")))

#Reformatting
df_munged_enriched_final=df_munged_enriched9.withColumn("loaddt",date_format(col("loaddt"),'yyyy/MM/dd')).withColumn("loadts",date_format(col("loadts"),'yyyy/MM/dd hh:mm'))

DSL Commented'''

#9. Aspirant's SQL Equivalent
#SQL wins due to familiarity
print("Add Columns - for this pipeline, I needed the load date and the timestamp")
#Add Columns - for this pipeline, I needed the load date and the timestamp
spark.sql("""
    CREATE OR REPLACE TEMP VIEW df_enriched1 AS
    SELECT *, '2024-05-23' AS loaddt, current_timestamp() AS loadts
    FROM df_standardized
""")

print("Rename Columns")
#Rename Columns
#DSL wins for simplicity of using withColumnRenamed
spark.sql("""
    CREATE OR REPLACE TEMP VIEW df_enriched2 AS
    SELECT cid as custid, fname AS firstname, lname AS lastname, prof as profession, age, sourcesystem, loaddt, loadts
    FROM df_enriched1
""")

print("Swapping Columns")
#Swapping Columns
print("By introducing temp column and dropping it")

#By introducing temp column and dropping it
#SQL wins due to simplicity, we can't achieve the same below result if we use DSL, it will leads to same column name displayed twice
spark.sql("""
    CREATE OR REPLACE TEMP VIEW df_enriched3 AS
    SELECT custid, lastname AS firstname, firstname AS lastname, profession, age, sourcesystem, loaddt, loadts
    FROM df_enriched2
""")

spark.sql("""
    CREATE OR REPLACE TEMP VIEW df_enriched4 AS
    SELECT custid, firstname, lastname, profession, age, sourcesystem, loaddt, loadts
    FROM df_enriched3
""")

#b. merge/Concat, split
print("merge")
#SQL wins due to simplicity
#merge
spark.sql("""
    CREATE OR REPLACE TEMP VIEW df_enriched5 AS
    SELECT *, CONCAT(firstname, '.', lastname, '.', custid, '@inceptez.com') AS mailid
    FROM df_enriched4
""")

print("Split")
#SQL wins due to simplicity
#Split
spark.sql("""
    CREATE OR REPLACE TEMP VIEW df_enriched6 AS
    SELECT *,
           SPLIT(mailid, '@')[0] AS userid,
           SPLIT(mailid, '@')[1] AS domain
    FROM df_enriched5
""")

print("Remove Columns")
#DSL wins
#Remove Columns
spark.sql("""
    CREATE OR REPLACE TEMP VIEW df_enriched7 AS
    SELECT custid, firstname, lastname, profession, age, sourcesystem, loaddt, loadts, mailid, userid, domain
    FROM df_enriched6
""")

#c. Type Casting, format & Schema Migration
#DSL and SQL are similar in terms of using function , but SQL wins due to simplicity
#Type casting
#We need to change the loaddt into date format
print("We need to change the loaddt into date format")
spark.sql("""
    CREATE OR REPLACE TEMP VIEW df_enriched8 AS
    SELECT custid, firstname, lastname, profession, age, sourcesystem, CAST(loaddt AS DATE) AS loaddt, loadts, mailid, userid, domain
    FROM df_enriched7
""")

#Extraction
print("Extraction of year from date")
spark.sql("""
    CREATE OR REPLACE TEMP VIEW df_enriched9 AS
    SELECT *, YEAR(loaddt) AS year
    FROM df_enriched8
""")

print("Reformatting")
#Reformatting
spark.sql("""
    CREATE OR REPLACE TEMP VIEW df_final AS
    SELECT custid, firstname, lastname, profession, age, sourcesystem,
           DATE_FORMAT(loaddt, 'yyyy/MM/dd') AS loaddt,
           DATE_FORMAT(loadts, 'yyyy/MM/dd hh:mm') AS loadts,
           mailid, userid, domain, year
    FROM df_enriched9
""")

#Execution of Final SQL Query
print("Execution of Final SQL Query")
spark.sql("""
    SELECT *
    FROM df_final
""").show()

print("2. Enrichment completed")

print("***************3. Data Customization & Processing (Business logics) -> "
      "Apply User defined functions and utils/FBP/modularization/reusable functions & reusable framework creation *********************")
print("Data Customization can be achived by using UDFs - User Defined Functions converted/registered from a Python Function")
print("User Defined Functions must be used only if it is Inevitable (un avoidable), because Spark consider UDF as a black box doesn't know how to "
      "apply optimization in the UDFs - When we have a custom requirement which cannot be achieved with the existing built-in functions.")

#3.a. How to create Python Functions & convert/Register it as a UDF to call these functions in the DSL/SQL Queries respectively
spark.conf.set("spark.sql.shuffle.partitions","5")
#I want to apply customization on the columns of our dataframe/temporary view using some custom functionalities
#Steps We are going to Follow
#Step 1. First evaluate whether the functionality we are going to apply is already available in a form of individual/composable built in functions
#Decide whether to use custom or builtin to customize & enrich the df_munged_enriched_final further

#Step 2. Create a python anonymous/defined (inline/common) functions - If we don't find builtin functions to achive the custom functionality defined by the business then
#Lets assume we don't have builtin function available, hence we are going with custom function
'''
def agecat(age):
    if age<=12:
        return "Childrens"
    else:
        return "Adults"
'''

'''DSL Commented
#IF WE ARE APPLYING UDF ON DATAFRAME USING DSL
#Step 3. Convert the python function to UDF (because python function is not serializable to use in the executor) - If we are going to apply this function in a DSL query
from pyspark.sql.functions import udf
from learn.reusableframework.reusable_functions import agecat
udf_agecat=udf(agecat)#converting python function as udf

#Step 4. Apply the udf on the dataframe columns using DSL
df_munged_enriched_customized=df_munged_enriched_final.withColumn("agecategory",udf_agecat(coalesce("age",lit(0))))

#OR

#IF WE ARE APPLYING UDF ON TEMPORARY VIEW USING SQL
#Step 3. Register the python function as UDF into the Metastore (because python function is not serializable to use in the executor & identifyable in the metastore by Spark SQ:) - If we are going to apply this function in a SQL query
from learn.reusableframework.reusable_functions import agecat
#udf_agecat=udf(agecat)
'''

def agecat(age):
    if age<=12:
        return "Childrens"
    else:
        return "Adults"
spark.udf.register("udf_sql_agecat",agecat)#registering python function as udf

#Step 4. Apply the udf on the Tempview columns using SQL

df_munged_enriched_customized=spark.sql("""select *,
                                           udf_sql_agecat(coalesce(cast(age as int),0)) agecategory 
                                           from df_final""")
print("UDF Applied SQL Output")
df_munged_enriched_customized.show(2)

'''DSL Commented
#3.b. Go for UDFs if it is in evitable – Because the usage of UDFs will degrade the performance of your pipeline

#Example of udf using anonymous function & performance optimization
filter_function=lambda x:True if x=='PILOT' else False
spark.udf.register("df_ready_udf_filter_function",filter_function)

df_filtered_pilot=df_munged_enriched_final.where("""df_ready_udf_filter_function(profession)=True""")
#Not optimistic to use because? Catalyst optimized is not used, data requires deserialization & serialization back.
#or
df_filtered_pilot=df_munged_enriched_final.where("""profession='PILOT'""")
#Optimistic to use because? Catalyst optimized is used, built in functions can be applied on the serialized data itself without deserializing.

#3.c. Creation of the Reusable Frameworks with Functions (Later - Modernization/Industrialization)
#We do this in Item 6

DSL Commented'''

print("***************4. Core Curation - Core Data Processing/Transformation (Level1) (Pre Wrangling)  -> "
      "filter, transformation, Grouping, Aggregation/Summarization, Sorting, Analysis/Analytics (EDA) *********************")

'''DSL Commented
#df_munged_enriched_customized - This DF has age with null and profession with blankspaces or nulls, and agecategory with adults & childrens

#transformation (na.fill, coalesce,cast, substr, to_date, concat, trim, isNull, lit, when(cond,value).when(cond2,value2).otherwise(default)), selectExpr(native sql query on the given row/columns))

df_munged_enriched_customized_transformed1=df_munged_enriched_customized.na.fill(0,["age"])#Age is transformed from null to 0

#Nulls are changed to na using coalesce
df_munged_enriched_customized_transformed2=df_munged_enriched_customized_transformed1.withColumn("profession",coalesce("profession",lit('na')))#Coalesce will return the first not null expression

#blank spaces are changed to na using case statement
#syntax of case statement ->
# SQL- (case when profession is null then 'na' when profession ='' then 'na' else profession end as profession)
# DSL- (df.withColumn("profession",when ("profession"=='','na').when ("profession"=='','na').when ("profession"=='','na').otherwise("profession")))
#DSL+SQL - selectExpr(case when profession is null then 'na' when profession='' then 'na' else profession)
df_munged_enriched_customized_transformed3=df_munged_enriched_customized_transformed2.withColumn("profession",when (trim("profession")=='','na').otherwise(col("profession")))
df_munged_enriched_customized_transformed3=df_munged_enriched_customized_transformed2.withColumn("profession",when (trim("profession")=='','na').when(coalesce("profession",lit(''))=='','na').otherwise(col("profession")))
df_munged_enriched_customized_transformed3.show()
#or
df_munged_enriched_customized_transformed3=df_munged_enriched_customized_transformed2.withColumn("profession",when (trim("profession")=='','na').when(col("profession").isNull(),'na').otherwise(col("profession"))).show()

#selectExpr function
df_munged_enriched_customized_transformed1.selectExpr("custid","firstname","lastname","case when profession is null then 'na' when profession='' then 'na' else profession end as profession").show()

#We need to handle nulls in age, blankspaces/nulls in profession and we want to introduce one more agecategory called teens
####################################################################################################################################################
#for just learning and understanding the speciality of coalesce/nvl/nvl2/nullif/decode/case...
df_munged_enriched_customized_transformed2_learning=df_munged_enriched_customized_transformed1.where("custid=4000002").withColumn("profession",lit(None))#creating sample data with profession as null
df_munged_enriched_customized_transformed3_learning=df_munged_enriched_customized_transformed1.where("custid=4000003").withColumn("profession",lit(''))#creating sample data with profession as ''
union_df=df_munged_enriched_customized_transformed2_learning.union(df_munged_enriched_customized_transformed3_learning)
#Standardizing & Filtering (all the nulls and blankspaces data has to be filtered)
union_df.where("coalesce(profession,'')=''").show()#coalesce is converting the nulls to '' to standardize the data, then we are filtering the ''
#or
union_df.where("profession is null or profession=''").show()
####################################################################################################################################################

#.withColumn("profession",coalesce("profession",lit('')))
#coalesce("profession",'')

#filter
df_munged_enriched_customized_filter_social=df_munged_enriched_customized_transformed3.where("""profession in ('DOCTOR','LAWYER','ECONOMIST','POLICE OFFICER')""")
df_munged_enriched_customized_filter_adults=df_munged_enriched_customized_transformed3.filter("""agecategory ='Adults' or age between 13 and 19""")

#case statement using selectExpr and then using DSL with().otherwise to modify the agecategory as childrens, teens and adults
#age with null as unknown (use coalesce or case), age <13 - childrens, age>=13 and age<=19 - teens , age>19 - adults df_munged_enriched_customized_transformed3

#transformation (after filter)

#Grouping, Aggregation/Summarization
#One column grouping & aggregation
df_aggr_count_prof_social=df_munged_enriched_customized_filter_social.groupby("profession").agg(count(lit(1)).alias("cnt"))
df_aggr_count_prof_social.show()

#SQL Equivalent
df_munged_enriched_customized_filter_social.createOrReplaceTempView("socialview")
spark.sql("select profession,count(1) from socialview group by profession").show()

#multiple columns grouping & 1 column aggregation
df_aggr_count_prof_age=df_munged_enriched_customized.groupby("profession","age").agg(count(lit(1)).alias("cnt"))

#multiple columns grouping & multiple column aggregation
df_multiple_group_aggr=df_munged_enriched_customized_filter_social.groupby("profession","year","agecategory").
agg(count(lit(1)).alias("cnt"),avg("age").alias("avg_age"),mean("age").alias("mean_age"),max("age").alias("max_age"),min("age").alias("min_age"))

#Grouping, Aggregation/Summarization, Sorting
df_multiple_group_aggr_sort_final=df_multiple_group_aggr.orderBy(["cnt","avg_age"])

#Enrichment (Derivation of KPIs (Key Performance Indicators))
df_munged_enriched_customized_transformed_kpi=df_munged_enriched_customized_transformed3.withColumn("agecatind",substring("agecategory",1,1))

#Format Modeling - convert the loaddt from yyyy/MM/dd to yyyy-MM-dd
df_munged_enriched_customized_transformed_kpi=df_munged_enriched_customized_transformed3.withColumn("loaddt",concat(substring("loaddt",1,4),lit('-'),substring("loaddt",6,2),lit('-'),substring("loaddt",9,2)).cast("date"))
#or
#to_date is an important function that will help us convert the date from any format to the standard format of yyyy-MM-dd
#date_format is the reverse of to_date function (help us convert the date from yyyy-MM-dd to any format as per our need)
df_munged_enriched_customized_transformed_kpi_format_final=df_munged_enriched_customized_transformed_kpi.withColumn("loaddt",to_date("loaddt",'yyyy/MM/dd'))

DSL Commented'''

#10. Aspirant's SQL Query
df_munged_enriched_customized.createOrReplaceTempView("df_munged_enriched_customized")

#SQL wins due to simplicity
print("Standardization 1 - Fill null values in 'age' column with 0")
# Standardization 1 - Fill null values in 'age' column with 0
spark.sql("""
    CREATE OR REPLACE TEMP VIEW df_munged_enriched_customized_transformed1 AS
    SELECT *, IFNULL(cast(regexp_replace(age,'[-]','') as int), 0) AS age1 FROM df_munged_enriched_customized
""")
spark.sql("select * from df_munged_enriched_customized_transformed1").show()
# Standardization 2 - Fill null values in 'profession' column with 'na'
print("Standardization 2 - Fill null values in 'profession' column with 'na'")
spark.sql("""
    CREATE OR REPLACE TEMP VIEW df_munged_enriched_customized_transformed2 AS
    SELECT custid,firstname,lastname,coalesce(profession,'na') as profession,age,
    sourcesystem,loaddt,loadts,mailid,userid,domain,year,agecategory,age1
 FROM df_munged_enriched_customized_transformed1
""")

print("Standardization 3 - Replace blank spaces in 'profession' column with 'na'")
# Standardization 3 - Replace blank spaces in 'profession' column with 'na'
spark.sql("""
    CREATE OR REPLACE TEMP VIEW df_munged_enriched_customized_transformed3 AS
    SELECT custid,firstname,lastname, CASE WHEN TRIM(profession) = '' THEN 'na' ELSE profession END AS profession,
    age,sourcesystem,loaddt,loadts,mailid,userid,domain,year,agecategory,age1 
    FROM df_munged_enriched_customized_transformed2
""")

print("Selection using selectExpr with case statement for profession column")


print("Filter null or blank values in profession column")
# Filter null or blank values in profession column
spark.sql("""
    SELECT * FROM df_munged_enriched_customized_transformed1 WHERE COALESCE(profession, '') = ''
""").show(2)

spark.sql("""SELECT * FROM df_munged_enriched_customized_transformed1 
WHERE profession IN ('DOCTOR', 'LAWYER', 'ECONOMIST', 'POLICE OFFICER')""").show()

print("Filtering based on selected professions and age")
# Filtering based on selected professions and age
spark.sql("""
    CREATE OR REPLACE TEMP VIEW df_munged_enriched_customized_filter_social AS
    SELECT * FROM df_munged_enriched_customized_transformed1
    WHERE profession IN ('DOCTOR', 'LAWYER', 'ECONOMIST', 'POLICE OFFICER')
""")

spark.sql("""
    CREATE OR REPLACE TEMP VIEW df_munged_enriched_customized_filter_adults AS
    SELECT * FROM df_munged_enriched_customized_transformed3
    WHERE agecategory = 'Adults' OR age BETWEEN 13 AND 19
""")

print("Grouping and aggregation")
#SQL wins due to familiarity and portability
# Grouping and aggregation
spark.sql("""
    CREATE OR REPLACE TEMP VIEW df_aggr_count_prof_social AS
    SELECT profession, COUNT(*) AS cnt
    FROM df_munged_enriched_customized_filter_social
    GROUP BY profession
""")
#df.groupBy("profession").agg(count(lit(1))).show()

#How to do achive grouping and aggregation with having clause in SQL and DSL?
#SQL wins due to familiarity and portability
#How can I apply filter on the aggregated data using SQL & DSL?
# In SQL we can use having clause, In DSL we can use just where itself
#I want to know the number of customer with a given profession is more than 10
spark.sql("""
    CREATE OR REPLACE TEMP VIEW df_aggr_count_prof_social AS
    SELECT profession, COUNT(*) AS cnt
    FROM df_munged_enriched_customized_filter_social
    GROUP BY profession
    HAVING count(*)>10
""")
#Equivalent DSL
from pyspark.sql.functions import *
df_aggr_count_prof_social=df_munged_enriched_customized.groupby("profession").\
    agg(count(lit(1)).alias("cnt")).where("cnt>10")
df_aggr_count_prof_social.show()

print("Filtering based on selected professions and age")
# Filtering based on selected professions and age
spark.sql("""
    CREATE OR REPLACE TEMP VIEW df_munged_enriched_customized_filter_social AS
    SELECT * FROM df_munged_enriched_customized_transformed1
    WHERE profession IN ('DOCTOR', 'LAWYER', 'ECONOMIST', 'POLICE OFFICER')
""")

spark.sql("""
    CREATE OR REPLACE TEMP VIEW df_munged_enriched_customized_filter_adults AS
    SELECT * FROM df_munged_enriched_customized_transformed3
    WHERE agecategory = 'Adults' OR age BETWEEN 13 AND 19
""")

print("Filtering based on selected professions and age")
# Filtering based on selected professions and age
spark.sql("""
    CREATE OR REPLACE TEMP VIEW df_munged_enriched_customized_filter_social AS
    SELECT * FROM df_munged_enriched_customized_transformed1
    WHERE profession IN ('DOCTOR', 'LAWYER', 'ECONOMIST', 'POLICE OFFICER')
""")

spark.sql("""
    CREATE OR REPLACE TEMP VIEW df_munged_enriched_customized_filter_adults AS
    SELECT * FROM df_munged_enriched_customized_transformed3
    WHERE agecategory = 'Adults' OR age BETWEEN 13 AND 19
""")

print("Grouping and aggregation")
# Grouping and aggregation
spark.sql("""
    CREATE OR REPLACE TEMP VIEW df_aggr_count_prof_social AS
    SELECT profession, COUNT(*) AS cnt
    FROM df_munged_enriched_customized_filter_social
    GROUP BY profession
""")

spark.sql("""
    SELECT profession, COUNT(1) AS cnt
    FROM df_munged_enriched_customized_filter_social
    GROUP BY profession
""").show(10)

print("Multiple columns grouping and aggregation")
#SQL wins due to familiarity and portability
# Multiple columns grouping and aggregation
spark.sql("""
    CREATE OR REPLACE TEMP VIEW df_multiple_group_aggr AS
    SELECT profession, year, agecategory, COUNT(*) AS cnt, AVG(age1) AS avg_age, MEAN(age1) AS mean_age, MAX(age1) AS max_age, MIN(age1) AS min_age
    FROM df_munged_enriched_customized_filter_social
    GROUP BY profession, year, agecategory
""")

print("Grouping, Aggregation/Summarization, Sorting")
# Grouping, Aggregation/Summarization, Sorting
spark.sql("""
    SELECT *
    FROM df_multiple_group_aggr
    ORDER BY cnt, avg_age
""").show()

print("Enrichment - Derivation of KPIs")
# Enrichment - Derivation of KPIs
spark.sql("""
    CREATE OR REPLACE TEMP VIEW df_munged_enriched_customized_transformed_kpi AS
    SELECT *, SUBSTRING(agecategory, 1, 1) AS agecatind
    FROM df_munged_enriched_customized_transformed3
""")

print("Format Modeling - Convert 'loaddt' to yyyy-MM-dd format")
# Format Modeling - Convert 'loaddt' to yyyy-MM-dd format
spark.sql("""
    CREATE OR REPLACE TEMP VIEW df_munged_enriched_customized_transformed_kpi_format_final AS
    SELECT *, TO_DATE(loaddt, 'yyyy/MM/dd') AS loaddt1
    FROM df_munged_enriched_customized_transformed_kpi
""")

'''DSL Commented
#Analysis/Analytics (EDA) (On top of Enriched, Customized & level1 Curated/Pre Wrangled data)
#Identifying corelation, most frequent occurance,
#Random Sampling
df_sample1=df_munged_enriched_customized.sample(.1)
df_sample2_1=df_munged_enriched_customized.sample(.2,1)
df_sample2_2=df_munged_enriched_customized.sample(.2,2)

#Frequency Analysis
df_freq1=df_munged_enriched_customized.freqItems(["age"],.1)
df_freq2=df_munged_enriched_customized.freqItems(["age"],.2)

#Corelation Analysis (Know the relation between 2 attributes)
df_munged_enriched_customized.withColumn("age2",col("age")).stat.corr("age","age2")#pearson corelation algorithm
df_munged_enriched_customized.stat.corr("custid","age")#least corelation

#Covariance Analysis
df_munged_enriched_customized.stat.cov("age","age")#Random corelation of data will happen

#Statistical Analysis
df_stats_summary=df_munged_enriched_customized.summary()

DSL Commented '''

#11. Aspirant's Equivalent SQL
print("Random Sampling")
#Random Sampling
spark.sql("""
    CREATE OR REPLACE TEMP VIEW df_sample1 AS
    SELECT *
    FROM df_munged_enriched_customized
    TABLESAMPLE (10 PERCENT)
""")
spark.sql("select * from df_sample1").show(2)

spark.sql("""
    CREATE OR REPLACE TEMP VIEW df_sample2_1 AS
    SELECT *
    FROM df_munged_enriched_customized
    TABLESAMPLE (20 PERCENT)
""")
spark.sql("select * from df_sample2_1").show()

spark.sql("""
    CREATE OR REPLACE TEMP VIEW df_sample2_2 AS
    SELECT *
    FROM df_munged_enriched_customized
    TABLESAMPLE (20 PERCENT)
""")
spark.sql("select * from df_sample2_2").show()

# Frequency Analysis (no direct function is available like freqItems in DSL
spark.sql("""CREATE OR REPLACE TEMP VIEW df_freq1 AS
    SELECT COLLECT_SET(age) AS age_freq
    FROM df_munged_enriched_customized
""")
spark.sql("select * from df_freq1").show(2)

print("DSL Wins - SQL Workaround (group by, agg, having, collect_set) for Frequency Analysis (no direct function is available like freqItems in DSL")
#SQL Wins - If we want with some customization of freq items identification
spark.sql("""
SELECT COLLECT_SET(age) AS age_freq
from (
select count(1) cnt,age from view1
group by age
having count(1)>100) temp
""").show()

spark.sql("""    CREATE OR REPLACE TEMP VIEW df_freq2 AS
    SELECT COLLECT_SET(age) AS age_freq
    FROM df_munged_enriched_customized
""")
spark.sql("select * from df_freq2").show(2)

#Cov and corr is not supported directly in Spark sql (DSL wins here)
print("Cov and corr is not supported directly in Spark sql (DSL Wins here directly)")
#Trend analysis, Market Basket Analysis, Feature selection, identifying relation between the fields, random sampling, statistical analysis, variance of related attributes...

print("*************** 5. Data Wrangling - Complete Data Curation/Processing/Transformation (Level2)  -> "
      "Joins, Lookup, Lookup & Enrichment, Denormalization, Windowing, Analytical, set operations "
      "Summarization (joined/lookup/enriched/denormalized) *********************")

#We are going to introduce new dataset and it is going to undergo all possible stages of our overall data transformation
#Munging -> Enrichment -> Customization -> Curation (Pre Wrangling) -> Wrangling
df_txns_raw=spark.read.csv("file:///home/hduser/sparkdata/txns",inferSchema=True).toDF("txnno","txndt","custid","amount","category","product","city","state","spendby")

'''DSL Commented
#The above data doesn't require Customization, but it requires all other stages...
#Munging
#We identified column name is not there, txnno is string type, nulls in the txnno
print(len(df_txns_raw.collect()))#Actual count
#or
print(df_txns_raw.count())
#95904
df_txns_raw.printSchema()
#
df_txns_raw.na.drop("any",subset=["txnno"]).count()#nulls are there in txnno
#95903
df_txns_raw.na.drop("any",subset=["txnno"]).where("upper(txnno)=lower(txnno)").count()#Nulls and string is there in txnno
#95902
df_txns_munged=df_txns_raw.na.drop("any",subset=["txnno"]).where("upper(txnno)=lower(txnno)")
#95902

#Enrichment (Format modeling and enrichment)
df_txns_munged_enriched1=df_txns_munged.withColumn("txnno",col("txnno").cast("int"))

#we need to do type casting of txndt to date format, but not possible directly because date in format of MM-dd-yyyy
#df_txns_munged_enriched.withColumn("txndt",col("txndt").cast("date")).show(2)#this will not work
#to_date function will change the date from unknown format (anything eg. MM-dd-yyyy or MM/dd/yy) to known format of 'yyyy-MM-dd'
df_txns_munged_enriched2=df_txns_munged_enriched1.withColumn("txndt",to_date("txndt",'MM-dd-yyyy'))#converts to MM-dd-yyyy -> yyyy-MM-dd
df_txns_munged_enriched2.show(2)
df_txns_munged_enriched2.printSchema()

#Let's Try to add more derived/enriched fields using some date functions
df_txns_munged_enriched3=df_txns_munged_enriched2.select("*",year("txndt").alias("year"),
                                                         month("txndt").alias("month")
                                                         ,add_months("txndt",1).alias("next_month_sameday"),
                                                         add_months("txndt",-1).alias("prev_month_sameday"),
                                                         last_day("txndt").alias("last_day_ofthe_month"),
                                                         date_add(last_day("txndt"),1).alias("first_day_ofthe_next_month"),
                                                        date_sub("txndt",2).alias("two_days_subtracted_dt"),
                                                         trunc("txndt","mm").alias("first_day_ofthe_month"),
                                                         trunc("txndt","yyyy").alias("first_day_ofthe_year"))


#Curation
#Grouping & Aggregation
df_txns_curated_agg=df_txns_munged_enriched2.groupBy("state").agg(sum("amount").alias("sum_amt"),max("amount").alias("max_amt"),min("amount").alias("min_amt"))
df_txns_curated_agg.show()

#Wrangling
#Pivoting with Grouping & Aggregation
df_txns_curated_agg=df_txns_munged_enriched2.groupBy("state","spendby").agg(sum("amount").alias("sum_amt"),max("amount").alias("max_amt"),min("amount").alias("min_amt"))
df_txns_curated_agg.show()
df_txns_curated_agg.show(100)

+--------------------+------------------+-------+-------+
|               state|           sum_amt|max_amt|min_amt|
+--------------------+------------------+-------+-------+
|           Wisconsin|223194.54999999996| 199.97|   5.15|
|                Ohio|359566.70000000007| 199.94|   5.07|
|             Florida| 532213.0099999999| 199.98|   5.13|
|                Utah|168689.39999999985| 199.89|   5.11|
|        Pennsylvania|178550.70000000016| 199.89|   5.01|
|              credit|             28.11|  28.11|  28.11|
|               Idaho| 87323.56999999998| 199.99|   6.06|



#Relate with these 2 dataframes to understand how pivot is working
df_txns_curated_agg_pivoted=df_txns_munged_enriched2.groupBy("state").pivot("spendby").agg(sum("amount").alias("sum_amt"),max("amount").alias("max_amt"),min("amount").alias("min_amt"))
df_txns_curated_agg_pivoted.show()
+--------------------+------------------+------------+------------+------------------+--------------+--------------+
|               state|      cash_sum_amt|cash_max_amt|cash_min_amt|    credit_sum_amt|credit_max_amt|credit_min_amt|
+--------------------+------------------+------------+------------+------------------+--------------+--------------+
|                Utah| 6137.310000000001|       49.77|        5.11|162552.08999999985|        199.89|          5.49|
|        Pennsylvania| 6345.200000000001|       49.55|        5.01|172205.50000000006|        199.89|          5.47|
|             Florida|19669.319999999996|       49.98|        5.19|         512543.69|        199.98|          5.13|
|               Idaho|2889.6399999999994|       49.96|        6.25| 84433.93000000001|        199.99|          6.06|
|           Wisconsin| 7641.409999999998|       49.75|        5.15|215553.13999999996|        199.97|          5.19|
|      North Carolina|           5765.98|        49.6|        5.27| 166431.2299999999|        199.64|          5.15|
|             Indiana| 3755.910000000001|       49.13|        8.38| 84482.05000000002|        199.81|          5.14|
|                Ohio|          13624.91|       49.99|        5.07|         345941.79|        199.94|          5.24|

DSL Commented '''

#12. Aspirant's SQL Equivalent

df_txns_raw.createOrReplaceTempView("df_txns_raw")

print("Check for nulls and string values in 'txnno' column")
spark.sql("""    
    CREATE OR REPLACE TEMP VIEW df_txns_munged AS
    SELECT * 
    FROM df_txns_raw
    WHERE txnno IS NOT NULL
    AND upper(txnno) = lower(txnno)""")

#df_txns_munged=df_txns_raw.na.drop("any",subset=["txnno"]).where("upper(txnno)=lower(txnno)")

print("Enrichment (Format modeling and enrichment)")
#Enrichment (Format modeling and enrichment)
#df_txns_munged_enriched1=df_txns_munged.withColumn("txnno",col("txnno").cast("int"))
spark.sql("""
    CREATE OR REPLACE TEMP VIEW df_txns_munged_enriched1 AS
    SELECT CAST(txnno AS INT) AS txnno, txndt, custid, amount, category, product, city, state, spendby
    FROM df_txns_munged""")

print("Convert 'txndt' column to date format")
#Convert 'txndt' column to date format
#df_txns_munged_enriched2=df_txns_munged_enriched1.withColumn("txndt",to_date("txndt",'MM-dd-yyyy'))#converts to MM-dd-yyyy -> yyyy-MM-dd
spark.sql("""
    CREATE OR REPLACE TEMP VIEW df_txns_munged_enriched2 AS
    SELECT txnno, TO_DATE(txndt, 'MM-dd-yyyy') AS txndt, custid, amount, category, product, city, state, spendby
    FROM df_txns_munged_enriched1""")

print("Add more derived/enriched fields using date functions")
#Add more derived/enriched fields using date functions
spark.sql("""CREATE OR REPLACE TEMP VIEW df_txns_munged_enriched3 AS
    SELECT *, YEAR(txndt) AS year, 
             MONTH(txndt) AS month,
             ADD_MONTHS(txndt, 1) AS next_month_sameday,
             ADD_MONTHS(txndt, -1) AS prev_month_sameday,
             LAST_DAY(txndt) AS last_day_ofthe_month,
             DATE_ADD(LAST_DAY(txndt), 1) AS first_day_ofthe_next_month,
             DATE_SUB(txndt, 2) AS two_days_subtracted_dt,
             TRUNC(txndt, "MM") AS first_day_ofthe_month,
             TRUNC(txndt, "YYYY") AS first_day_ofthe_year
    FROM df_txns_munged_enriched2;
""").show(10)

print("Grouping & Aggregation")
#Curation
#Grouping & Aggregation
#df_txns_curated_agg=df_txns_munged_enriched2.groupBy("state").agg(Sum("amount").alias("sum_amt"),max("amount").alias("max_amt"),min("amount").alias("min_amt"))
spark.sql("""CREATE OR REPLACE TEMP VIEW df_txns_curated_agg AS
    SELECT state, Sum(amount) sum_amt, MAX(amount) max_amt, MIN(amount) AS min_amt
    FROM df_txns_munged_enriched2
    GROUP BY state""")


print("Pivoting with Grouping & Aggregation")
#Wrangling
#Pivoting with Grouping & Aggregation
spark.sql("""CREATE OR REPLACE TEMP VIEW df_txns_curated_agg AS
    SELECT state, spendby, SUM(amount) AS sum_amt, MAX(amount) AS max_amt, MIN(amount) AS min_amt
    FROM df_txns_munged_enriched2
    GROUP BY state, spendby""")

spark.sql("select * from df_txns_curated_agg").show()

'''DSL Commented
#Stopping my Transaction data processing here....

#Let's UNDERSTAND & LEARN about different types of joins
#Joins - What is join? Join is the operation of horizontaly connecting the related or un related dataset appling matching
# or un matching conditions or without conditions to get the integrated/expand/denormalized view of the multiple datasets
#Join is used for integrating multiple datasets horizontally
#Important Interview Questions for joins?
#1. What are the types of SQL joins you know/used?
#2. In the below Quick scenario, try to find the respective join produces howmuch result?
#Scenario: I have 2 datasets, datset1 contains 10 rows, dataset2 contains 15 rows, matching rows between these dataset is 8 rows,
#dataset1 has 2 rows not matching with dataset2 and dataset2 has 7 rows not matching with dataset1

df_left=df_munged_enriched_customized.where("custid in (4000001,4000002,4000003)").select("custid","firstname","lastname","age","profession")
df_right=df_munged_enriched_customized.where("custid in (4000003,4000011,4000012,4000013)").select("custid","firstname","lastname","age","profession")

#How to write join syntax in DSL
#Simple Syntax
df_left.join(df_right).show()#cross join/cartesian join/product join (not good to use)
df_left.join(df_right,on="custid",how="inner").show()#Simple syntax with the usage of all parameters for join
#'inner', 'outer', 'full', 'fullouter', 'full_outer', 'leftouter', 'left', 'left_outer', 'rightouter', 'right', 'right_outer', 'leftsemi', 'left_semi', 'semi', 'leftanti', 'left_anti', 'anti', 'cross'
# In the above simple syntax we have few challenges to resolve, Above simple syntax will work only (if there are common columns with the same name used for join, if there is no other data columns between dataframes with same name)
df_joined=df_left.join(df_right,on="custid",how="inner")
df_joined.select("custid","age").show()#This will not work due to ambiguity issue
#pyspark.sql.utils.AnalysisException: Reference 'age' is ambiguous, could be: mun_enr_view.age, mun_enr_view.age.

#Comprehensive Syntax
df_right=df_right.withColumnRenamed("custid","cid")
df_joined=df_left.join(df_right,on=([col("custid")==col("cid")]),how="inner")#If I don't have common column names for joining
#If I don't have common column names for joining & If we have common columns between 2 dataframes
df_joined=df_left.alias("l").join(df_right.alias("r"),on=([col("l.custid")==col("r.cid")]),how="inner")#Comprehensive syntax to use
df_joined.select("l.custid","l.age","r.age").show()

#Wanted to apply multiple conditions
df_joined=df_left.alias("l").join(df_right.alias("r"),on=([(col("l.custid")==col("r.cid")) & (col("l.firstname")==col("r.firstname"))]),how="inner")#Comprehensive syntax to use

#NOW LET'S LEARN THE DIFFERENT TYPES OF JOINS BEHAVIOR
df_left.show()
+-------+---------+--------+----+----------+
| custid|firstname|lastname| age|profession|
+-------+---------+--------+----+----------+
|4000001|    Chung|Kristina|  55|     PILOT|
|4000002|     Chen|   Paige|null|     ACTOR|
|4000003|    irfan| mohamed|  41|        IT|
+-------+---------+--------+----+----------+
>>> df_right.show()
+-------+---------+--------+---+----------+
|    cid|firstname|lastname|age|profession|
+-------+---------+--------+---+----------+
|4000003|    irfan| mohamed| 41|        IT|
|4000011| McNamara| Francis| 47| THERAPIST|
|4000012|   Raynor|   Sandy| 26|    WRITER|
|4000013|     Moon|  Marion| 41| CARPENTER|
+-------+---------+--------+---+----------+


#Inner join - Data from dataset1 joined with dataset2 using some join/filter conditions that returns matching values between 2 datasets
df_joined=df_left.alias("l").join(df_right.alias("r"),on=([col("l.custid")==col("r.cid")]),how="inner")
df_joined.select("l.custid","r.cid","l.age","r.age").show()#1 row returned

#Outer Join
#Left join - returns ALL values from dataset1 (left) and nulls for unmatched value of dataset2
df_joined=df_left.alias("l").join(df_right.alias("r"),on=([col("l.custid")==col("r.cid")]),how="left_outer")
df_joined.select("l.custid","r.cid",col("l.age").alias("l_age"),col("r.age").alias("r_age")).show()#3 row returned

#Right join - Returns ALL values from dataset2 (right) and nulls for unmatched value of dataset1
df_joined=df_left.alias("l").join(df_right.alias("r"),on=([col("l.custid")==col("r.cid")]),how="right")
df_joined.select("l.custid","r.cid",col("l.age").alias("l_age"),col("r.age").alias("r_age")).show()#4 row returned

#Full Outer - Feturns ALL values from dataset1 (left) & dataset1 (right) with nulls for unmatched value of both datasets
df_joined=df_left.alias("l").join(df_right.alias("r"),on=([col("l.custid")==col("r.cid")]),how="outer")
df_joined.select("l.custid","r.cid",col("l.age").alias("l_age"),col("r.age").alias("r_age")).show()#6 row returned (1 is common row, left 2, right 3)

#Semi Join - Returns MATCHING values from dataset1 (left) alone (Works like a suquery with in condition)
df_joined=df_left.alias("l").join(df_right.alias("r"),on=([col("l.custid")==col("r.cid")]),how="semi")
#df_joined.select("l.custid","r.cid",col("l.age").alias("l_age"),col("r.age").alias("r_age")).show()
df_joined.select("l.custid",col("l.age").alias("l_age")).show()

#or refer the below sql query with in condition

#or (achieving semi join using inner join (but inner join can produce data from right df also))
df_joined=df_left.alias("l").join(df_right.alias("r"),on=([col("l.custid")==col("r.cid")]),how="inner")
df_joined.select("l.custid",col("l.age")).show()

#There is no join like Right semi/anti? Yes we have, but not in spark
#If No right semi/anti is there, then how can i achieve it? just swap the dataframes
df_joined=df_right.alias("l").join(df_left.alias("r"),on=([col("r.custid")==col("l.cid")]),how="semi")
df_joined.select("r.cid",col("r.age").alias("r_age")).show()

#Anti Join - Returns UN-MATCHING values from dataset1 (left) alone (Works like a suquery with not in condition)
df_joined=df_left.alias("l").join(df_right.alias("r"),on=([col("l.custid")==col("r.cid")]),how="anti")
#df_joined.select("l.custid","r.cid",col("l.age").alias("l_age"),col("r.age").alias("r_age")).show()
df_joined.select("l.custid",col("l.age").alias("l_age")).show()

#or refer the below sql query with not in condition

#or (achieving anti join using left join (but left join can produce data from right df also))
df_joined=df_left.alias("l").join(df_right.alias("r"),on=([col("l.custid")==col("r.cid")]),how="left")
df_joined.where("r.cid is null").select("l.custid",col("l.age")).show()

#Self Join - Data from dataset1 joined with dataset1 using some join conditions that returns matching values between 2 datasets
df_left1=df_left.withColumn("custid_ref",col("custid")-1)#Creating hierarchical data for using in self join
df_joined=df_left1.alias("l").join(df_left1.alias("r"),on=([col("l.custid")==col("r.custid_ref")]),how="inner")
df_joined.where("r.cid is null").select("l.custid",col("l.age")).show()
df_joined.selectExpr("concat(r.custid,' is referred by ',l.custid)").show(10,False)

#Cross Join - Data from dataset1 joined with dataset2 WITHOUT using any join conditions that returns  values between 2 datasets in an iteration of (10*15=150 rows will be returned)
df_joined=df_left.alias("l").join(df_right.alias("r"))

DSL Commented'''

#13. Aspirant's SQL Query
print("Understanding Different SQL Joins")
df_left=spark.sql("select * from df_munged_enriched_customized").where("custid in (4000001,4000002,4000003)").select("custid","firstname","lastname","age","profession")
df_right=spark.sql("select * from df_munged_enriched_customized").where("custid in (4000003,4000011,4000012,4000013)").select("custid","firstname","lastname","age","profession")
df_left.createOrReplaceTempView("left_view")
df_right.withColumnRenamed("custid","cid").createOrReplaceTempView("right_view")

print("natural/inner join")
spark.sql("""select l.*,r.* 
             from left_view l join right_view r 
             on l.custid=r.cid 
             and l.firstname=r.firstname
             """).show()

#df_joined=df_left.alias("l").
# join(df_right.alias("r"),
# on=([(col("l.custid")==col("r.cid")) & (col("l.firstname")==col("r.firstname"))])
# ,how="inner")#Comprehensive syntax to use

print("Semi join (subquery with exists condition)")
#Semi join (subquery with exists condition)
spark.sql("select l.* from left_view l where exists (select cid from right_view r where r.cid=l.custid)").show() #exists will check the first occurance of the data
#or
spark.sql("select l.* from left_view l where l.custid in (select cid from right_view r)").show()#in will check all values are present or not
#Interview Question: Difference between in and exists condition
#Exists is more efficient because it will return the boolean true if the first occurance is found rather than the values
#Exists is more efficient because it will return the boolean true/false rather than the values

print("Anti join (subquery with not exists condition)")
#Anti join (subquery with not exists condition)
spark.sql("select l.* from left_view l where not exists (select cid from right_view r where r.cid=l.custid)").show()
#or
spark.sql("select l.* from left_view l where l.custid not in (select cid from right_view r)").show()

#14. Aspirant SQL Equivalent
print("Create temporary views for the left and right dataframes")
# Create temporary views for the left and right dataframes
from pyspark.sql.functions import *
df_left=spark.sql("select * from df_raw").where("cid in (4000001,4000002,4000003)").select(col("cid").alias("custid"),col("fname").alias("firstname"),col("lname").alias("lastname"),"age",col("prof").alias("profession"))
df_right=spark.sql("select * from df_raw").where("cid in (4000003,4000011,4000012,4000013)").select(col("cid").alias("custid"),col("fname").alias("firstname"),col("lname").alias("lastname"),"age",col("prof").alias("profession"))
df_left.createOrReplaceTempView("df_left")
df_right.createOrReplaceTempView("df_right")

# Cross join / Cartesian join
print("Cross join / Cartesian join")
spark.sql("""
    SELECT *
    FROM df_left l
    CROSS JOIN df_right r
""").show()

print("Inner join")
# Inner join
spark.sql("""
    SELECT *
    FROM df_left l
    INNER JOIN df_right r
    ON l.custid = r.custid
""").show()

print("Select specific columns (with alias to resolve ambiguity)")
# Select specific columns (with alias to resolve ambiguity)
spark.sql("""
    SELECT l.custid, l.age AS l_age, r.age AS r_age
    FROM df_left l
    INNER JOIN df_right r
    ON l.custid = r.custid
""").show()

# Comprehensive syntax for join without common column names

print("Apply multiple conditions in join")
# Apply multiple conditions in join
spark.sql("""
    SELECT l.*, r.*
    FROM df_left l
    INNER JOIN df_right r
    ON l.custid = r.custid AND l.firstname = r.firstname
""").show()

print("Left Join")
# Left join
spark.sql("""
    SELECT l.custid, r.custid AS r_cid, l.age AS l_age, r.age AS r_age
    FROM df_left l
    LEFT JOIN df_right r
    ON l.custid = r.custid
""").show()

print("Right Join")
# Right join
spark.sql("""
    SELECT l.custid, r.custid AS r_cid, l.age AS l_age, r.age AS r_age
    FROM df_left l
    RIGHT JOIN df_right r
    ON l.custid = r.custid
""").show()

print("Full Join")
# Full outer join
spark.sql("""
    SELECT l.custid, r.custid AS r_cid, l.age AS l_age, r.age AS r_age
    FROM df_left l
    FULL OUTER JOIN df_right r
    ON l.custid = r.custid
""").show()

# Semi join
spark.sql("""
    SELECT l.*
    FROM df_left l
    WHERE EXISTS (SELECT r.custid FROM df_right r WHERE r.custid = l.custid)
""").show()

print("Achieving semi join using inner join")
# Achieving semi join using inner join
spark.sql("""
    SELECT l.custid, l.age
    FROM df_left l
    INNER JOIN df_right r
    ON l.custid = r.custid
""").show()

print("Anti Join")
# Anti join
spark.sql("""
    SELECT l.*
    FROM df_left l
    WHERE NOT EXISTS (SELECT r.custid FROM df_right r WHERE r.custid = l.custid)
""").show()

#or

spark.sql("""
    SELECT l.custid, l.age
    FROM df_left l
    SEMI JOIN df_right r
    ON l.custid = r.custid
""").show()

print("Achieving anti join using left join")
# Achieving anti join using left join
spark.sql("""
    SELECT l.custid, l.age
    FROM df_left l
    LEFT JOIN df_right r
    ON l.custid = r.custid
    WHERE r.custid IS NULL
""").show()

#OR
spark.sql("""
    SELECT l.custid, l.age
    FROM df_left l
    ANTI JOIN df_right r
    ON l.custid = r.custid
""").show()

print("Self join")
# Self join
spark.sql("""
    CREATE OR REPLACE TEMP VIEW df_left1 AS
    SELECT custid, firstname, lastname, age, profession, (custid - 1) AS custid_ref
    FROM df_left
""")

spark.sql("""
    SELECT l.*, r.*
    FROM df_left1 l
    INNER JOIN df_left1 r
    ON l.custid = r.custid_ref
""").show()

print("Cross join without conditions")
# Cross join without conditions
spark.sql("""
    SELECT *
    FROM df_left l
    JOIN df_right r
""").show()

print("SQL query for semi join (subquery with exists condition)")
# SQL query for semi join (subquery with exists condition)
spark.sql("""
    SELECT l.*
    FROM df_left l
    WHERE EXISTS (SELECT r.custid FROM df_right r WHERE r.custid = l.custid)
""").show()

print("SQL query for anti join (subquery with not exists condition)")
# SQL query for anti join (subquery with not exists condition)
spark.sql("""
    SELECT l.*
    FROM df_left l
    WHERE NOT EXISTS (SELECT r.custid FROM df_right r WHERE r.custid = l.custid)
""").show()


'''DSL Commented

#Further we are going to marry the curated cust data (df_munged_enriched_customized_transformed_kpi_format_final)
# with the curated transaction data (df_txns_munged_enriched2)
#Joins, Lookup, Lookup & Enrichment, Denormalization

#APPLICATION OF DIFFERENT TYPES OF JOINS IN OUR REAL LIFE (DBS, DWH, LAKEHOUSES, BIGLAKES, DATALAKES, DELTALAKES)
#-----------------------------------------------------------------------------------------------------------------
#Joins - To Enrich our Data pipeline or to apply different scenarios of our dataset
df_munged_enriched_customized_transformed_kpi_format_final.show(2)
df_txns_munged_enriched3.show(2)

#Rename the above DFs to use it simply
df_cust_dim_wrangle_ready=df_munged_enriched_customized_transformed_kpi_format_final
df_txns_fact_wrangle_ready=df_txns_munged_enriched3

#Lets assume we are in the month of January 2012
#Doing some EDA to understand howmany customers I have
print(df_cust_dim_wrangle_ready.count())#9910 overall customers that I have in my business
df_txns_fact_wrangle_ready.groupBy("year","month").agg(sum("amount")).show()#12 months worth of sales data of 2011 I have

print("a. Lookup (Joins)")
#lookup is an activity of identifying some existing data with the help of new data
#lookup can be achived using joins (semi join, anti join, Left join, inner join)

print("Identify the count of Active Customers or Dormant Customers")
#take latest 1 month worth of txns data and compare with the customer data
#Lets assume we are in the month of January 2012 - Wanted to do some analysis on the last month sales happened
df_txns_2011_12=df_txns_fact_wrangle_ready.where("year=2011 and month=12")

#Active Customers (customers did transactions in last 1 month)
#Using semi join (high priority) why? Because semi will display only the left table data checking right table data only once using exists function in the background...
df_cust_dim_wrangle_ready.alias("l").join(df_txns_2011_12.alias("r"),on="custid",how="semi").count() #5475 active customers

#Using Left join (mid priority)
df_cust_dim_wrangle_ready.alias("l").join(df_txns_2011_12.alias("r"),on="custid",how="left").where("r.custid is not null").\
    select("custid").distinct().count()#5475 active customers

#Using inner join (least priority)
df_cust_dim_wrangle_ready.alias("l").join(df_txns_2011_12.alias("r"),on="custid",how="inner").\
    select("custid").distinct().count()#5475 active customers

#Dormant Customers (customers didn't do transactions in last 1 month)
#The customers who did not do any transactions last month are dormant customers
#Using semi join (high priority) why? Because semi will display only the left table data checking right table data only once using exists function in the background...
df_cust_dim_wrangle_ready.alias("l").join(df_txns_2011_12.alias("r"),on="custid",how="anti").count() #4435 dormant/inactive customers

#Using Left join (mid priority)
df_cust_dim_wrangle_ready.alias("l").join(df_txns_2011_12.alias("r"),on="custid",how="left").where("r.custid is null").\
    select("custid").distinct().count()#5475 active customers

#Using Left join (low priority)
df_cust_dim_wrangle_ready.alias("l").join(df_txns_2011_12.alias("r"),on="custid",how="inner").where("r.custid is null").\
    select("custid").distinct().count()#5475 dormant customers

print("b. Lookup & Enrichment (Joins)")
#lookup is an activity of checking whether the given value is matching and enriching is process of getting/using the lookup data ()
#Transaction data of last 1 month I am considering above
#Returns the details of the transactions did by the given customers
#We are doing a lookup based on custid and enriching amount from the transaction data

#We may left join if we need all customer info with or without enriched amount
df_cust_dim_wrangle_ready.alias("l").join(df_txns_2011_12.alias("r"),on="custid",how="left").select("custid","profession","amount").show()

#We may inner join if we need only customer info with enriched amount is available
df_cust_dim_wrangle_ready.alias("l").join(df_txns_2011_12.alias("r"),on="custid",how="inner").select("custid","profession","amount").show()

#We may right join if we need show all transactions regardless whether we have customer info is present or not
df_cust_dim_wrangle_ready=df_cust_dim_wrangle_ready.where("custid between 4002613 and 4004613")
#Orphan customers (transactions happened in (callcenter/web) but no customer info captured in our DL later in my customer table)
df_cust_dim_wrangle_ready.alias("l").join(df_txns_2011_12.alias("r"),on="custid",how="right").where("custid is null").select("custid","profession","amount").show()

print("c. Denormalization - (Schema Modeling) - (Creating Wide Data/Fat Data) - (Star Schema model-Normalized) - "
      "Join Scenario to provide DENORMALIZATION view or flattened or wide tables or fat tables view of data to the business for faster access without doing join")

#Flattening/Widing/Fat table/Denormalization
df_cust_txns_denorm_fact_inner=df_cust_dim_wrangle_ready.join(df_txns_fact_wrangle_ready,on=("custid"))#Inner/Natural using custid
#Above join returns only matching cust data with the transaction
df_cust_txns_denorm_fact_left=df_cust_dim_wrangle_ready.join(df_txns_fact_wrangle_ready,on=("custid"),how="left")#Left join using custid
#Above join returns only all cust data with the applicable transaction, else null transactions

#df_cust_txns_denorm_fact_inner.write.saveAsTable("cust_trans_fact")#in stage 6 we will do this..

#We can use full or right join also if we want to know all cust/all trans data regardles whether it matches or not

#Star Schema Model
df_cust_dim_wrangle_ready.write.saveAsTable("cust_dim")
df_txns_fact_wrangle_ready.write.saveAsTable("txnss_fact")

#benefits (flattened/widened/denormalized data)
#Once for all we do costly join and flatten the data and store it in the persistant storage (hive/athena/bq/synapse/redshift/parquet)
#Not doing join again and again when consumer does analysis, rather just query the flattened joined data
#drawback(flattened/widened/denormalized data)
#1. duplicate data
#2. occupies storage space

print("Equivalent SQL for Denormalization (denormalization helps to create a single view for faster query execution without joining)")


DSL Commented '''


#15. Aspirant's SQL Equivalent
# Create temp views for SQL usage
spark.sql("create or replace temp view cust_dim_wrangle_ready as select * from df_munged_enriched_customized_transformed_kpi_format_final ")
spark.sql("create or replace temp view txns_fact_wrangle_ready as select * from df_txns_munged_enriched3 ")

# Filter transactions to December 2011
spark.sql("create or replace temp view txns_2011_12 as select * from df_txns_munged_enriched3 where year=2011 and month=12")


print("a. Lookup (Joins)")
# Active Customers
# Using semi join (high priority)
active_customers_semi = spark.sql("""
    SELECT COUNT(*)
    FROM cust_dim_wrangle_ready l
    SEMI JOIN txns_2011_12 r
    ON l.custid = r.custid
""")
active_customers_semi.show()

# Using left join (mid priority)
active_customers_left = spark.sql("""
    SELECT COUNT(DISTINCT l.custid)
    FROM cust_dim_wrangle_ready l
    LEFT JOIN txns_2011_12 r
    ON l.custid = r.custid
    WHERE r.custid IS NOT NULL
""")
active_customers_left.show()

# Using inner join (least priority)
active_customers_inner = spark.sql("""
    SELECT COUNT(DISTINCT l.custid)
    FROM cust_dim_wrangle_ready l
    INNER JOIN txns_2011_12 r
    ON l.custid = r.custid
""")
active_customers_inner.show()

# Dormant Customers
# Using anti join (high priority)
dormant_customers_anti = spark.sql("""
    SELECT COUNT(*)
    FROM cust_dim_wrangle_ready l
    ANTI JOIN txns_2011_12 r
    ON l.custid = r.custid
""")
dormant_customers_anti.show()

# Using left join (mid priority)
dormant_customers_left = spark.sql("""
    SELECT COUNT(DISTINCT l.custid)
    FROM cust_dim_wrangle_ready l
    LEFT JOIN txns_2011_12 r
    ON l.custid = r.custid
    WHERE r.custid IS NULL
""")
dormant_customers_left.show()

# b. Lookup & Enrichment (Joins)
print("b. Lookup & Enrichment (Joins)")
# Left join for all customer info with or without enriched amount
left_join = spark.sql("""
    SELECT l.custid, l.profession, r.amount
    FROM cust_dim_wrangle_ready l
    LEFT JOIN txns_2011_12 r
    ON l.custid = r.custid
""")
left_join.show()

# Inner join for customer info with enriched amount
inner_join = spark.sql("""
    SELECT l.custid, l.profession, r.amount
    FROM cust_dim_wrangle_ready l
    INNER JOIN txns_2011_12 r
    ON l.custid = r.custid
""")
inner_join.show()

# Right join for all transactions
spark.sql("create or replace temp view cust_dim_wrangle_ready_filtered as select * from cust_dim_wrangle_ready where custid BETWEEN 4002613 AND 4004613")

right_join_orphans = spark.sql("""
    SELECT r.custid, l.profession, r.amount
    FROM cust_dim_wrangle_ready_filtered l
    RIGHT JOIN txns_2011_12 r
    ON l.custid = r.custid
    WHERE l.custid IS NULL
""")

right_join_orphans.show()

print("c. Denormalization - (Schema Modeling)")
# Denormalization view or flattened tables view
# Inner join (matching customer data with transactions)
denorm_fact_inner = spark.sql("""
    SELECT *
    FROM cust_dim_wrangle_ready l
    INNER JOIN txns_fact_wrangle_ready r
    ON l.custid = r.custid
""")
denorm_fact_inner.show()

# full join (all customer data with all transactions)
denorm_fact_left = spark.sql("""
    SELECT *
    FROM cust_dim_wrangle_ready l
    FULL OUTER JOIN txns_fact_wrangle_ready r
    ON l.custid = r.custid
""")
denorm_fact_left.show()

'''DSL Commented

print("d. Windowing Functionalities")
#clauses/functions - row_number(), rank(), dense_rank() - over(), partition(), orderBy()
#Application of Windowing Functions
#Windowing functions are very very important functions, used for multiple applications
#Creating Surrogate key/sequence number/identifier fields/primary key
#Creating change versions (SCD2) - Seeing this in project
#Identifying & Eliminating Duplicates -
#Top N Analysis

df_txns_curated_agg.show()#This is not having any surrogate/sequence number column
print("aa.How to generate seq or surrogate key column on the ENTIRE data kept in a single partition")
df_txns_curated_agg.withColumn("st_spendby_key",monotonically_increasing_id()).show(100)#Multiple partitions with scattered sequence number
df_txns_curated_agg.coalesce(1).withColumn("st_spendby_key",monotonically_increasing_id()).show(100)#Single partition with ordered sequence number

df_txns_curated_agg.coalesce(1).withColumn("st_spendby_key",monotonically_increasing_id()).select(col("st_spendby_key").alias("seq_no"),"*").drop("st_spendby_key").show(100)
#or
df_txns_curated_agg.coalesce(1).select(monotonically_increasing_id().alias("seq_no"),"*").show(100)


df_raw1=spark.read.csv("file:///home/hduser/sparkdata/custsmodified",inferSchema=True,header=True).toDF("cid","fname","lname","age","prof")
df_txns_raw=spark.read.csv("file:///home/hduser/sparkdata/txns",inferSchema=True).toDF("txnno","txndt","custid","amount","category","product","city","state","spendby")
df_right=df_raw1
df_left=df_txns_raw
df_joined=df_left.alias("l").join(df_right.alias("r"),on=([col("l.custid")==col("r.cid")]),how="inner")

print("aa.How to generate seq or surrogate key column on the ENTIRE data sorted based on the transaction date")
df_txns_curated_agg.coalesce(1).orderBy("sum_amt").withColumn("st_spendby_key",monotonically_increasing_id()).show(100)#Single partition with ordered sequence number
df_txns_curated_agg.coalesce(1).withColumn("st_spendby_key",monotonically_increasing_id()).show(100)#Single partition with ordered sequence number

#or By using window function

print("bb. How to generate seq or surrogate key column accross the CUSTOMERs data sorted based on the transaction date")
from pyspark.sql.window import Window
df_joined_seqno=df_joined.select(row_number().over(Window.orderBy("txndt")).alias("rno"),"*")
df_joined_seqno.show()

print("Least 3 transactions in our overall transactions")
df_joined_least3=df_joined.select(row_number().over(Window.orderBy("amount")).alias("rno"),"*").where("rno<=3")#One person with one unique rank
df_joined_least3.show()
df_joined_least3_drank=df_joined.select(dense_rank().over(Window.orderBy("amount")).alias("drank"),"*").where("drank<=3")#same value should have equal rank and next value jump to next sequence rank
df_joined_least3_drank.show()
df_joined_least3_rank=df_joined.select(rank().over(Window.orderBy("amount")).alias("rank"),"*").where("rank<=18")##same value should have equal rank and next value jump to next count(previous rank)+1 rank
df_joined_least3_rank.show()

print("Top 3 transactions made by the given customer")
df_joined.where("custid in (4000329,4001198)").orderBy("custid").show(100)
df_joined_cust_top3=df_joined.where("custid in (4000329,4001198)").select(row_number().over(Window.partitionBy("custid").orderBy(desc("amount"))).alias("rno"),"*").where("rno<=3")
df_joined_cust_top3.show()

print("Top 1 transaction amount made by the given customer 4000000")
df_joined.where("custid in (4000001)").groupBy("custid").agg(max("amount")).show()
df_joined_cust_top3=df_joined.where("custid in (4000001)").select(row_number().over(Window.partitionBy("custid").orderBy(desc("amount"))).alias("rno"),"*").where("rno=1")
df_joined_cust_top3.show()

print("Top 2nd transaction amount made by the given customer 4000001")
df_joined_cust_top3=df_joined.where("custid in (4000001)").select(row_number().over(Window.partitionBy("custid").orderBy(desc("amount"))).alias("rno"),"*").where("rno=2")
df_joined_cust_top3.show()

print("How to de-duplicate based on certain fields eg. show me whether the given customer have played a category of games atleast once")
df_joined_cust_top3=df_joined.where("category in ('Games')").select(row_number().over(Window.partitionBy("custid").orderBy(desc("amount"))).alias("rno"),"*").where("rno=2")
df_joined_cust_top3.show()

#or
#Equivalent to writing Subquery in DSL (through join only we can achieve)
df_custid_txnno=df_joined.where("category in ('Games')").groupBy("custid").agg(max("txnno").alias("txnno"))
df_all_cust=df_custid_txnno.join(df_joined,on=(["custid","txnno"]),how='inner')#As we have both DFs have common column names custid,txnno we can just mention like this....
#or
df_all_cust=df_custid_txnno.alias("a").join(df_joined.alias("b"),on=([(col("a.custid")==col("b.custid")),(col("a.txnno")==col("b.txnno"))]),how='inner')

df_joined.createOrReplaceTempView("df_joined_view")
spark.sql("""select * from df_joined_view where concat(custid,txnno) 
             in (select concat(custid,mtxnno) from 
             (select custid,max(txnno) mtxnno from df_joined_view where category in ('Games') group by custid)t)""").count()

#or

DSL Commented '''

#17. Aspirant's SQL Equivalent

# Generate sequence or surrogate key column on the ENTIRE data kept in a single partition

# Single partition with ordered sequence number
spark.sql("""
    SELECT monotonically_increasing_id() AS st_spendby_key, *
    FROM df_txns_curated_agg
""").coalesce(1).show(10)

# Generate sequence or surrogate key column on the ENTIRE data sorted based on the transaction date
spark.sql("""
    SELECT monotonically_increasing_id() AS st_spendby_key, *
    FROM df_txns_curated_agg
    ORDER BY sum_amt
""").coalesce(1).show(10)

# Generate sequence or surrogate key column across the CUSTOMERS data sorted based on the transaction date
#df_joined.createOrReplaceTempView("df_joined")

df_raw1=spark.read.csv("file:///home/hduser/sparkdata/custsmodified",inferSchema=True,header=True).toDF("custid","fname","lname","age","prof")
df_txns_raw=spark.read.csv("file:///home/hduser/sparkdata/txns",inferSchema=True).toDF("txnno","txndt","cid","amount","category","product","city","state","spendby")
df_raw1.createOrReplaceTempView("df_right")
df_txns_raw.createOrReplaceTempView("df_left")
#df_joined=df_left.alias("l").join(df_right.alias("r"),on=([col("l.custid")==col("r.cid")]),how="inner")

spark.sql(""" 
    SELECT *
    FROM df_left l
    INNER JOIN df_right r
    ON l.cid = r.custid""").createOrReplaceTempView("df_joined")

spark.sql("""
    SELECT row_number() OVER (ORDER BY txndt) AS rno, *
    FROM df_joined
""").show()
#from pyspark.sql.window import Window
#DSL SYNTAX - select(row_number().over(Window.orderBy("txndt")))
# Least 3 transactions in overall transactions
spark.sql("""
    select * from (SELECT row_number() OVER (ORDER BY amount) AS rno, *
    FROM df_joined) temp
    WHERE rno <= 3
""").show()

# Least 3 transactions using dense rank
spark.sql("""
    select * from (SELECT dense_rank() OVER (ORDER BY amount) AS drank, *
    FROM df_joined) temp
    WHERE drank <= 3
""").show()
#from pyspark.sql.window import Window
#DSL SYNTAX - select(dense_rank().over(Window.orderBy("txndt")))

# Least 3 transactions using rank
spark.sql("""
    select * from (SELECT rank() OVER (ORDER BY amount) AS rank, *
    FROM df_joined) temp
    WHERE rank <= 18
""").show()
#DSL SYNTAX - select(rank().over(Window.orderBy("txndt")))
# Top 3 transactions made by a given customer
spark.sql("""
select * from (
    SELECT row_number() OVER(PARTITION BY custid ORDER BY amount DESC) AS rno, *
    FROM df_joined
    WHERE custid IN (4000329, 4001198)) temp
    where rno <= 3
""").show()
#DSL SYNTAX - select(row_number().over(Window.partitionBy("custid").orderBy(desc("amount"))))

# Top 1 transaction amount made by a given customer
spark.sql("""
    SELECT custid, MAX(amount) AS max_amount
    FROM df_joined
    WHERE custid in ( 4000001,4000002)
    GROUP BY custid
""").show()

# Top 2nd transaction amount made by a given customer
spark.sql("""
   select * from (
    SELECT row_number() OVER (PARTITION BY custid ORDER BY amount DESC) AS rno, *
    FROM df_joined
    WHERE custid = 4000001) temp
    where rno = 2
""").show()

# De-duplicate based on certain fields
spark.sql("""
    select * from (SELECT row_number() OVER (PARTITION BY custid ORDER BY amount DESC) AS rno, *
    FROM df_joined
    WHERE category = 'Games') temp
    where rno = 2
""").show()

# De-duplicate based on certain fields using join
df_custid_txnno = spark.sql("select * from df_joined").where("category in ('Games')").groupBy("custid").agg(max("txnno").alias("txnno"))
df_custid_txnno.createOrReplaceTempView("df_custid_txnno")

spark.sql("""
    SELECT b.*
    FROM df_custid_txnno a
    INNER JOIN df_joined b
    ON a.custid = b.custid AND a.txnno = b.txnno
""").show()


print("E. Analytical Functionalities")
''' DSL Commented
#Reporting Side regularly used Analytical functions (pivot, rollup & cube)
#Regularly used analytical functions (cov, corr, sample, max/min/sum/count/avg/mean/mode/variance/stddev/freqitems...)

#Lead and Lag functions are HIERARCHICAL ANALYSIS/COMPARISON FUNCTIONS
#Recently using the below analytical functions in my project (lead & lag)
#Eg. We receive customer events data from different sources (learn eventts 8.00/8.40 -> analyse 8.10/8.10-> book 8.20/8.35-> shipped 8.30/8.30-> receive)

#DATA IMPUTATION/FABRICATION
#Channel of Interaction
#1 learn eventts 8.00/8.40 - arrived 8.10
#2 analyse 8.10/8.10 - 8.10 lag(prev) 8.40 - wrong time - swap 8.40 with 8.10 - arrive 8.40 - swap 8.40 with 8.30
#3 book 8.20/8.35 - 8.35 lag(prev) 8.40 - wrong time - swap 8.40 with 8.35 - arrive 8.40 - arrive 8.30 - swap 8.35 with 8.30 - arrive 8.35
#4 shipped 8.30/8.30 - 8.30 lag(prev) 8.40 - wrong time - swap 8.40 with 8.30 - arrive 8.40

print("What is the purchase pattern of the given customer, whether his buying potential is increased or decreased transaction by transaction")
df_distinct_custid4000001=df_joined.distinct().where("custid in (4000001)")
df_distinct_custid4000001.orderBy("txndt").show()
df_joined_prev_txn_amount=df_distinct_custid4000001.select("*",lag("amount",1,0).over(Window.orderBy("txndt")).alias("prev_amt"))
df_joined_prev_next_txn_amount=df_distinct_custid4000001.select("*",lag("amount",1,0).over(Window.orderBy("txndt")).alias("prev_amt"),lead("amount",1,0).over(Window.orderBy("txndt")).alias("next_amt"))

#Purchase pattern of the customer
#Identify the first and last transactions made by the customer
df_joined_prev_next_txn_amount_purchase_pattern_des=df_joined_prev_next_txn_amount.withColumn("first_last_trans",when(col("prev_amt")==0,'first_trans').when(col("next_amt")==0,'last_trans').otherwise("subsequent trans"))

#Identify the purchase capacity of the customer
df_joined_prev_next_txn_amount_purchase_capacity_des=df_joined_prev_next_txn_amount_purchase_pattern_des.\
    withColumn("purchase_capacity_prev_curr",
               when((col("amount")>=col("prev_amt")),'increased from previous').
               when((col("amount")<col("prev_amt")),'decreased from previous')).\
    withColumn("purchase_capacity_next_curr",
               when((col("amount")<=col("next_amt")),'decreased from next').
               when((col("amount")>col("next_amt")),'increased from next'))

# Cube & Rollup functions are (Comprehensive/Settlement/Drill Down or Up/Granularity) Grouping & Aggregation functions
#Cube - Viewing something (data) in all dimensions with all permutation and combination
#Rollup - Aggregate the data at every possible levels of aggregation

df_joined.where("custid in (4000001,4005737) and category in ('Outdoor Recreation','Games')").groupBy("category","spendby").agg(sum("amount"))
df_rollup=df_joined.where("custid in (4000001,4005737) and category in ('Outdoor Recreation','Games')").rollup("category","spendby").agg(sum("amount"))

#whatever we provide in cube we can see all the combination of results
df_cube=df_joined.where("custid in (4000001,4005737) and category in ('Outdoor Recreation','Games')").cube("category","spendby").agg(sum("amount"))

#or to understand cube better, lets do the aggregation at every level manually, not by using cube
df_joined.where("custid in (4000001,4005737) and category in ('Outdoor Recreation','Games')").groupBy("category","spendby").agg(sum("amount")).show()
df_joined.where("custid in (4000001,4005737) and category in ('Outdoor Recreation','Games')").groupBy("category").agg(sum("amount")).show()
df_joined.where("custid in (4000001,4005737) and category in ('Outdoor Recreation','Games')").groupBy("spendby").agg(sum("amount")).show()
df_joined.where("custid in (4000001,4005737) and category in ('Outdoor Recreation','Games')").agg(sum("amount")).show()

print("F. Set Operations - (union/unionall/unionbyname)/(subtract/minus/difference)/intersection")
#Thumb rules : Number, order and datatype of the columns must be same, otherwise unionbyname with missingcolumns function you can use.

df1=df_joined.where("custid in (4000001)").select("state","category","spendby")
df2=df_joined.where("custid in (4005737)").select("state","category","spendby")
print(df1.count())
print(df2.count())

print("Complete customers (with duplicates) across the 2 dataframes state,category,spendby")
df3=df1.union(df2)#union and unionall both are same
print(df3.count())

print("Complete customers (without duplicates) across the 2 dataframes state,category,spendby")
df3=df1.union(df2).distinct()

print("Common customer transactions across the 2 dataframes state,category,spendby")
df3=df1.intersect(df2)
df3.show()

print("Find which customer have did more un common transactions (Excess data in df1 than df2 & vice versa)")
df3=df1.subtract(df2)
df3.show()
df3=df2.subtract(df1)
df3.show()

#Aspirant's SQL Equivalent

from pyspark.sql.functions import lag, lead, when, col, sum
from pyspark.sql.window import Window

# Assuming df_joined is your DataFrame
#df_joined.createOrReplaceTempView("df_joined")

DSL Commented'''

# Purchase pattern of the customer
# Identify the first and last transactions made by the customer
purchase_pattern_query = """
SELECT *,
       CASE WHEN prev_amt = 0 THEN 'first_trans'
            WHEN next_amt = 0 THEN 'last_trans'
            ELSE 'subsequent trans' END AS first_last_trans
FROM (
    SELECT *,
           LAG(amount, 1, 0) OVER (ORDER BY txndt) AS prev_amt,
           LEAD(amount, 1, 0) OVER (ORDER BY txndt) AS next_amt
    FROM (
        SELECT DISTINCT *,
               ROW_NUMBER() OVER (PARTITION BY custid ORDER BY txndt) AS row_num
        FROM df_joined
        WHERE custid in ( 4000001)
    ) AS distinct_custid
) AS prev_next_txn_amount
ORDER BY txndt
"""
purchase_pattern_df = spark.sql(purchase_pattern_query)
purchase_pattern_df.show()

# Purchase capacity of the customer
purchase_capacity_query = """
SELECT *,
       CASE WHEN amount >= prev_amt THEN 'increased from previous'
            ELSE 'decreased from previous' END AS purchase_capacity_prev_curr,
       CASE WHEN amount <= next_amt THEN 'decreased from next'
            ELSE 'increased from next' END AS purchase_capacity_next_curr
FROM (
    SELECT *,
           LAG(amount, 1, 0) OVER (ORDER BY txndt) AS prev_amt,
           LEAD(amount, 1, 0) OVER (ORDER BY txndt) AS next_amt
    FROM (
        SELECT DISTINCT *,
               ROW_NUMBER() OVER (PARTITION BY custid ORDER BY txndt) AS row_num
        FROM df_joined
        WHERE custid = 4000001
    ) AS distinct_custid
) AS prev_next_txn_amount
"""
purchase_capacity_df = spark.sql(purchase_capacity_query)
purchase_capacity_df.show()

# Cube & Rollup functions
rollup_query = """
SELECT category, spendby, SUM(amount)
FROM df_joined
WHERE custid IN (4000001, 4005737) AND category IN ('Outdoor Recreation', 'Games')
GROUP BY ROLLUP (category, spendby)
"""
rollup_df = spark.sql(rollup_query)
rollup_df.show()

cube_query = """
SELECT category, spendby, SUM(amount)
FROM df_joined
WHERE custid IN (4000001, 4005737) AND category IN ('Outdoor Recreation', 'Games')
GROUP BY CUBE (category, spendby)
"""
cube_df = spark.sql(cube_query)
cube_df.show()

# Set Operations
df1 = spark.sql("SELECT state, category, spendby FROM df_joined WHERE custid = 4000001")
df2 = spark.sql("SELECT state, category, spendby FROM df_joined WHERE custid = 4005737")

print("Complete customers (with duplicates) across the 2 dataframes state, category, spendby")
union_query = """
SELECT state, category, spendby FROM df_joined WHERE custid = 4000001
UNION ALL
SELECT state, category, spendby FROM df_joined WHERE custid = 4005737
"""
union_result = spark.sql(union_query)
union_result.createOrReplaceTempView("union_result")
total_with_duplicates = union_result.count()
print(total_with_duplicates)

print("Complete customers (without duplicates) across the 2 dataframes state, category, spendby")
distinct_query = "SELECT DISTINCT state, category, spendby FROM union_result"
distinct_result = spark.sql(distinct_query)
distinct_result.createOrReplaceTempView("distinct_result")
total_without_duplicates = distinct_result.count()
print(total_without_duplicates)

print("Common customer transactions across the 2 dataframes state, category, spendby")
intersect_query = """
SELECT state, category, spendby FROM df_joined WHERE custid = 4000001
INTERSECT
SELECT state, category, spendby FROM df_joined WHERE custid = 4005737
"""
intersect_result = spark.sql(intersect_query)
intersect_result.show()

print("Find which customer have did more un common transactions (Excess data in df1 than df2 & vice versa)")
subtract_query_df1 = """
SELECT state, category, spendby FROM df_joined WHERE custid = 4000001
MINUS
SELECT state, category, spendby FROM df_joined WHERE custid = 4005737
"""
subtract_result_df1 = spark.sql(subtract_query_df1)
subtract_result_df1.show()

subtract_query_df2 = """
SELECT state, category, spendby FROM df_joined WHERE custid = 4005737
MINUS
SELECT state, category, spendby FROM df_joined WHERE custid = 4000001
"""
subtract_result_df2 = spark.sql(subtract_query_df2)
subtract_result_df2.show()



'''DSL Commented
#6. Data Persistance (LOAD)-> Data Publishing & Consumption - Enablement of the Cleansed, transformed and analysed data as a Data Product.
#Consumer Data Product

#a. Discovery layer/Consumption/Publishing/Gold Layer we can enable the data
#Model building, Slicing/Dicing, Drilldown/Drillup, ....
#Star Schema Model
df_cust_dim_wrangle_ready.write.saveAsTable("cust_dim")
df_txns_fact_wrangle_ready.write.saveAsTable("txnss_fact")

#b. Outbound/Egress

df_domant_customer=df_cust_dim_wrangle_ready.alias("l").join(df_txns_2011_12.alias("r"),on="custid",how="inner").where("r.custid is null").\
    select("custid").distinct()

df_domant_customer.write.csv("file:///home/hduser/customer_dormant")#send this data to the consumer downstream system using some tools, ftp mechanism, mail...


#c. Reports/Exports
df_cust_dim_wrangle_ready.write.saveAsTable("cust_dim")
df_txns_fact_wrangle_ready.write.saveAsTable("txnss_fact")

from configparser import *
def writeRDBMSData(df,propfile,db,tbl,mode):
    config = ConfigParser()
    config.read(propfile)
    driver=config.get("DBCRED", 'driver')
    host=config.get("DBCRED", 'host')
    port=config.get("DBCRED", 'port')
    user=config.get("DBCRED", 'user')
    passwd=config.get("DBCRED", 'pass')
    url=host+":"+port+"/"+db
    url1 = url+"?user="+user+"&password="+passwd
    df.write.jdbc(url=url1, table=tbl, mode=mode, properties={"driver": driver})

df_rollup.write.mode("overwrite").jdbc(url="jdbc:mysql://34.16.71.60:3306/ordersproducts?user=irfan",table="trans_rollup",\
                            properties={"driver":"com.mysql.cj.jdbc.Driver","password":"Inceptez@123","numPartitions":"2",\
                                        "batchsize":"10000","truncate":"true"})#RDBMS Reporting Server

df_cube.write.mode("overwrite").jdbc(url="jdbc:mysql://34.16.71.60:3306/ordersproducts?user=irfan",table="trans_cube",\
                            properties={"driver":"com.mysql.cj.jdbc.Driver","password":"Inceptez@123","numPartitions":"2",\
                                        "batchsize":"10000","truncate":"true"})#RDBMS Reporting Server

#d. Schema migration
df_munged_enriched_customized.write.json("file:///home/hduser/munged_enriched_customized")

#All We need from here is....
#1. Our Aspirant's are going to create a document about this bb2 program comprise of
#Transformation Terminologies (eg. wrangling, munging),
# Business logics (aggregation, grouping, standardizing),
# Interview scenarios ,
# Function we used (withcolumn, select)

#2. What's next?
#a. We are going to see the SQL equivalent of the the above DSLs to learn both DSL & SQL relatively
#b. We are going to see the modernized/standardized/generic function based BB2 program in a industry standard way..

DSL Commented'''

'''
DSL vs SQL
###########
Both PySpark DSL and PySpark SQL have their strengths and are suited for different scenarios. 
PySpark DSL is more flexible and better integrated with Python code, 
making it suitable for complex transformations and programmatic data processing. 
PySpark SQL, on the other hand, is more readable for those with SQL experience and is suitable for ad-hoc queries 
and simpler transformations. 
Understanding when and how to use each interface can help you write more efficient and maintainable PySpark code.
'''