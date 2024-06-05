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

# Script generated for node Accelerometer_Landing
Accelerometer_Landing_node1717513163392 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://my-s3-bucket-for-udacity-project-3/accelerometer/landing/"], "recurse": True}, transformation_ctx="Accelerometer_Landing_node1717513163392")

# Script generated for node Customer_Trusted
Customer_Trusted_node1717513121504 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://my-s3-bucket-for-udacity-project-3/customer/trusted/"], "recurse": True}, transformation_ctx="Customer_Trusted_node1717513121504")

# Script generated for node SQL Query
SqlQuery2081 = '''
select a.*
from accelerometer_landing a
join customer_trusted c
on a.user = c.email;
'''
SQLQuery_node1717513221620 = sparkSqlQuery(glueContext, query = SqlQuery2081, mapping = {"accelerometer_landing":Accelerometer_Landing_node1717513163392, "customer_trusted":Customer_Trusted_node1717513121504}, transformation_ctx = "SQLQuery_node1717513221620")

# Script generated for node Accelerometer_Trusted
Accelerometer_Trusted_node1717513408974 = glueContext.getSink(path="s3://my-s3-bucket-for-udacity-project-3/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="Accelerometer_Trusted_node1717513408974")
Accelerometer_Trusted_node1717513408974.setCatalogInfo(catalogDatabase="udacity-pj-3",catalogTableName="accelerometer_trusted")
Accelerometer_Trusted_node1717513408974.setFormat("json")
Accelerometer_Trusted_node1717513408974.writeFrame(SQLQuery_node1717513221620)
job.commit()