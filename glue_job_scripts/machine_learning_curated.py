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

# Script generated for node Step_Trainer_Trusted
Step_Trainer_Trusted_node1717515665011 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://my-s3-bucket-for-udacity-project-3/step_trainer/trusted/"], "recurse": True}, transformation_ctx="Step_Trainer_Trusted_node1717515665011")

# Script generated for node Accelerometer_Trusted
Accelerometer_Trusted_node1717515693302 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://my-s3-bucket-for-udacity-project-3/accelerometer/trusted/"], "recurse": True}, transformation_ctx="Accelerometer_Trusted_node1717515693302")

# Script generated for node SQL Query
SqlQuery2187 = '''
select
    s.sensorReadingTime as sensorReadingTime,
    s.serialNumber as serialNumber,
    s.distanceFromObject as distanceFromObject,
    a.user as user,
    a.x as x,
    a.y as y,
    a.z as z
from step_trainer_trusted s
join accelerometer_trusted a
on s.sensorReadingTime = a.timestamp;
'''
SQLQuery_node1717515728540 = sparkSqlQuery(glueContext, query = SqlQuery2187, mapping = {"step_trainer_trusted":Step_Trainer_Trusted_node1717515665011, "accelerometer_trusted":Accelerometer_Trusted_node1717515693302}, transformation_ctx = "SQLQuery_node1717515728540")

# Script generated for node Machine_Learning_Curated
Machine_Learning_Curated_node1717515897856 = glueContext.getSink(path="s3://my-s3-bucket-for-udacity-project-3/step_trainer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="Machine_Learning_Curated_node1717515897856")
Machine_Learning_Curated_node1717515897856.setCatalogInfo(catalogDatabase="udacity-pj-3",catalogTableName="machine_learning_curated")
Machine_Learning_Curated_node1717515897856.setFormat("json")
Machine_Learning_Curated_node1717515897856.writeFrame(SQLQuery_node1717515728540)
job.commit()