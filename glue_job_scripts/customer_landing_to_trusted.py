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

# Script generated for node Customer_Landing
Customer_Landing_node1717512190541 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://my-s3-bucket-for-udacity-project-3/customer/landing/"], "recurse": True}, transformation_ctx="Customer_Landing_node1717512190541")

# Script generated for node SQL Query
SqlQuery0 = '''
select * from myDataSource
where shareWithResearchAsOfDate is not null
and shareWithResearchAsOfDate != 0;
'''
SQLQuery_node1717512779757 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":Customer_Landing_node1717512190541}, transformation_ctx = "SQLQuery_node1717512779757")

# Script generated for node Customer_Trusted
Customer_Trusted_node1717512915451 = glueContext.getSink(path="s3://my-s3-bucket-for-udacity-project-3/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="Customer_Trusted_node1717512915451")
Customer_Trusted_node1717512915451.setCatalogInfo(catalogDatabase="udacity-pj-3",catalogTableName="customer_trusted")
Customer_Trusted_node1717512915451.setFormat("json")
Customer_Trusted_node1717512915451.writeFrame(SQLQuery_node1717512779757)
job.commit()