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

# Script generated for node Customer_Curated
Customer_Curated_node1717514590236 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://my-s3-bucket-for-udacity-project-3/customer/curated/"], "recurse": True}, transformation_ctx="Customer_Curated_node1717514590236")

# Script generated for node Step_Trainer_Landing
Step_Trainer_Landing_node1717514557766 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://my-s3-bucket-for-udacity-project-3/step_trainer/landing/"], "recurse": True}, transformation_ctx="Step_Trainer_Landing_node1717514557766")

# Script generated for node SQL Query
SqlQuery2090 = '''
select s.*
from step_trainer_landing s
join customer_curated c
on s.serialNumber = c.serialNumber;
'''
SQLQuery_node1717514611629 = sparkSqlQuery(glueContext, query = SqlQuery2090, mapping = {"step_trainer_landing":Step_Trainer_Landing_node1717514557766, "customer_curated":Customer_Curated_node1717514590236}, transformation_ctx = "SQLQuery_node1717514611629")

# Script generated for node Step_Trainer_Trusted
Step_Trainer_Trusted_node1717514817190 = glueContext.getSink(path="s3://my-s3-bucket-for-udacity-project-3/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="Step_Trainer_Trusted_node1717514817190")
Step_Trainer_Trusted_node1717514817190.setCatalogInfo(catalogDatabase="udacity-pj-3",catalogTableName="step_trainer_trusted")
Step_Trainer_Trusted_node1717514817190.setFormat("json")
Step_Trainer_Trusted_node1717514817190.writeFrame(SQLQuery_node1717514611629)
job.commit()