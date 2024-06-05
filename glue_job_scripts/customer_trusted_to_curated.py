import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Customer_Trusted
Customer_Trusted_node1717513892696 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://my-s3-bucket-for-udacity-project-3/customer/trusted/"], "recurse": True}, transformation_ctx="Customer_Trusted_node1717513892696")

# Script generated for node Step_Trainer_Landing
Step_Trainer_Landing_node1717513920766 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://my-s3-bucket-for-udacity-project-3/step_trainer/landing/"], "recurse": True}, transformation_ctx="Step_Trainer_Landing_node1717513920766")

# Script generated for node Distinct_Step_Trainer_Landing
SqlQuery2088 = '''
select distinct(serialNumber)
from step_trainer_landing;

'''
Distinct_Step_Trainer_Landing_node1717514209648 = sparkSqlQuery(glueContext, query = SqlQuery2088, mapping = {"step_trainer_landing":Step_Trainer_Landing_node1717513920766}, transformation_ctx = "Distinct_Step_Trainer_Landing_node1717514209648")

# Script generated for node SQL Query
SqlQuery2087 = '''
select c.*
from customer_trusted c
join step_trainer_landing s
on c.serialNumber = s.serialNumber;
'''
SQLQuery_node1717513970139 = sparkSqlQuery(glueContext, query = SqlQuery2087, mapping = {"customer_trusted":Customer_Trusted_node1717513892696, "step_trainer_landing":Distinct_Step_Trainer_Landing_node1717514209648}, transformation_ctx = "SQLQuery_node1717513970139")

# Script generated for node Customer_Curated
Customer_Curated_node1717514111305 = glueContext.getSink(path="s3://my-s3-bucket-for-udacity-project-3/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="Customer_Curated_node1717514111305")
Customer_Curated_node1717514111305.setCatalogInfo(catalogDatabase="udacity-pj-3",catalogTableName="customer_curated")
Customer_Curated_node1717514111305.setFormat("json")
Customer_Curated_node1717514111305.writeFrame(SQLQuery_node1717513970139)
job.commit()