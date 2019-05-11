from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from datetime import datetime
from io import StringIO
import botocore.session
import sys
import json

def cleanUp(client,config_table,config_common):

	"""
	Completes the entire process
	"""
	response=b_listObject(client,config_common["bucket"],config_common["temp_op_key"])
	for i in response["Contents"]:
		if "_SUCCESS" in i["Key"]:
			delete_extra=b_deleteObject(client,config_common["bucket"],i["Key"]) #delete success file
		else:
			del_path=config_table["read_location"].split(config_common["bucket"]+"/")[1]
			dele_alpha=b_deleteObject(client,config_common["bucket"],del_path) #delete file in external table folder
			dest_val=config_table["read_location"].split(config_common["bucket"]+"/")[1]
			c_resp=b_copyObject(client,config_common["bucket"],config_common["bucket"]+"/"+i["Key"],dest_val) # copy file in in external table folder
			dele_tempop=b_deleteObject(client,config_common["bucket"],i["Key"]) # delete result file in temp
			dele_tempsrc=b_deleteObject(client,config_common["bucket"],config_common["temp_src_key"])


def b_copyObject(client,bucket,copySource,key):
	response = client.copy_object(Bucket=bucket,CopySource=copySource,Key=key)

def b_deleteObject(client,bucket,key):
	response=client.delete_object(Bucket=bucket, Key=key)
	return response

def b_listObject(client,bucket,prefix):
	response = client.list_objects(Bucket=bucket,Prefix=prefix)
	return response

def insertTable(spark,alpha,delta):
	"""
	Insert into table 
	"""
	querry="""insert overwrite table abc.employee 
	select * from {0} 
	UNION
	select * from {1}""".format(alpha,delta)
	result=spark.sql(querry)
	return result
	

def updateTable(spark,alpha,delta,key_val):
	"""
	update the given table based on the key in the metadata
	"""
	if len(key_val)==1:
		querry="""select * 
						  from {0} org
						  where org.{2} not in 
						 (select del.{2} 
						 from {1} del
						 where del.{2}=old.{2})
						 
						 UNION ALL
						 
						 select * 
						 from {1} del
						 """.format(alpha,delta,key_val[0])
					 
		result=spark.sql(querry)
	elif len(key_val)==2:
		querry="""select * 
						  from {0} org
						  where org.{2} not in 
						 (select del.{2} 
						 from {1} del
						 where del.{2}=old.{2} and del.{3}=old.{3} )
						 
						 UNION ALL
						 
						 select * 
						 from {1} del
						
						 """.format(alpha,delta,key_val[0],key_val[1])
					 
		result=spark.sql(querry)
				
	elif len(key_val)==3:
		querry="""select * 
						  from {0} org
						  where org.{2} not in 
						 (select del.{2} 
						 from {1} del
						 where del.{2}=old.{2} and del.{3}=old.{3} and del.{4}=old.{4} )
						 UNION ALL
						 select * 
						 from {1} del
						 """.format(alpha,delta,key_val[0],key_val[1],key_val[2])
					 
		result=spark.sql(querry)
		
	return result
	
def deleteTable(spark,alpha,delta,key_val):	
	"""
	Delete Rows
	"""
	if len(key_val)==1:
		querry="""select * 
						  from {0} org
						  where org.{2} not in 
						 (select del.{2} 
						 from {1} del
						 where del.{2}=old.{2})
						 """.format(alpha,delta,key_val[0])
					 
		result=spark.sql(querry)
	elif len(key_val)==2:
		querry="""select * 
						  from {0} org
						  where org.{2} not in 
						 (select del.{2} 
						 from {1} del
						 where del.{2}=old.{2} and del.{3}=old.{3} )
						 """.format(alpha,delta,key_val[0],key_val[1])
					 
		result=spark.sql(querry)
				
	elif len(key_val)==3:
		querry="""select * 
						  from {0} org
						  where org.{2} not in 
						 (select del.{2} 
						 from {1} del
						 where del.{2}=old.{2} and del.{3}=old.{3} and del.{4}=old.{4} )
						 """.format(alpha,delta,key_val[0],key_val[1],key_val[2])
					 
		result=spark.sql(querry)
	return result
	
	

def loadMetadata():
	"""
	load metadata info in the json file 
	"""
	with open('/home/hadoop/nikhil/info.json') as f:
		data = json.load(f)
		return data["metadata"]

def main():
	
	spark = SparkSession.builder.appName("Te").enableHiveSupport().getOrCreate()
	config=loadMetadata()
	session = botocore.session.get_session()
	client = session.create_client('s3')
	"""
	check whether the input argument i.e table name match the table name in metadata info
	"""
	
	if len(sys.argv)<=2:
		print("Inputs did not match!!!!")
		spark.stop()
	
	tableExistFlag=False	
	for key in config.keys():
		if key == sys.argv[1]
			tableExistFlag=True
	
	if not tableExistFlag:
		print("Incorrect table name!!!!!")
		spark.stop()
	"""
	Copy From External table location to your location(temp_src/temp_src.csv)
	"""
	config_table=config[sys.argv[1]]
	config_common=config["common"]
	
	bucket=config_common["bucket"]
	copySource=config_table["read_locaton"].split('//')[1]
	key=config_common["temp_src_key"]
	response=b_copyObject(client,bucket,copySource,key)
	
	"""
	Creating Dataframe
	"""
	delta_df = spark.read.format("csv").option("header", "true").load(config_common["temp_read_location"])
	delta_df.createOrReplaceTempView(config_common["delta_name"])
	
	alpha_df = spark.read.format("csv").option("header", "true").load(config_table["read_location"])
	alpha_df.createOrReplaceTempView(config_common["alpha_name"])
	
	key_val=config_table["lookup"]
	
	"""
	Handle delete/update/modify operations
	"""
	if sys.argv[2]=="U" or sys.argv[2]=="u":
		result=updateTable(spark,config_common["alpha_name"],config_common["delta_name"],key_val)
		result.coalesce(1).write.mode("overwrite").csv(config_common["temp_write_location"])
		cleanUp(client,config_table,config_common)
		
	elif sys.argv[2]=="I" or sys.argv[2]=="i":
		result=insertTable(spark,config_common["alpha_name"],config_common["delta_name"])
		result.coalesce(1).write.mode("overwrite").csv(config_common["temp_write_location"])
		cleanUp(client,config_table,config_common)
		
	elif sys.argv[2]=="D" or sys.argv[2]=="d":
		result=deleteTable(spark,config_common["alpha_name"],config_common["delta_name"],key_val)
		result.coalesce(1).write.mode("overwrite").csv(config_common["temp_write_location"])
		cleanUp(client,config_table,config_common)
	else:
		print("Unknown Operation ")
	
	
	


if __name__ == "__main__":
	main()

