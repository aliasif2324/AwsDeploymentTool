
# ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- 
# Author: Ali Asif
# Date: 1 December 2019
# ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- 

# ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- 
# AWS S3 and EC2
# Assume we have multiple gz zipped files sitting on S3. 
# Create a script that will fire up an EC2 instance and run Problem 2.py 
# ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- 

import boto3
import botocore
import paramiko

# setup boto3 for managing amazon ec2 instance / machines
ec2 = boto3.client('ec2')


# this will create key used for ec2 instance
def createKey():
	print("createKey")
	try: 
		outfile = open('TestKey.pem','w')
		response = ec2.delete_key_pair(KeyName='aws_dre_assignment1')
		key_pair = ec2.create_key_pair(KeyName='aws_dre_assignment1')
		response = ec2.describe_key_pairs()
		print(response['KeyPairs'])
		KeyPairOut = str(key_pair['KeyMaterial'])
		outfile.write(KeyPairOut)
	except:
		print("key exists")


# deploy file to ec2 and execute
def deploy_to_aws_generic(ssh, ec2_instance, bucket, command):
	print ("deploy_to_aws_generic  " +  command )
	stdin, stdout, stderr = ssh.exec_command(command)


def deploy_to_aws(ssh, ec2_instance, bucket, script, extention):
	print ("deploy_to_aws  " +  script )
	execute_script_in_ec2_generic(ssh, ec2_instance, "ucd1", "wget https://" + bucket + ".s3.amazonaws.com/" + script + extention)

	export_script = "tar -xf " + script + extention #  ".tar.gz"
	deploy_to_aws_generic(ssh, ec2_instance, bucket, export_script)


def execute_script_in_ec2_generic(ssh, ec2_instance, bucket, command):
	print ("execute_script_in_ec2_generic " + command)	
	stdin, stdout, stderr = ssh.exec_command(command)
	# print ("execute command - flush")
	stdin.flush()
	# print ("execute command - data read")
	data = stdout.read().splitlines()
	
	# print ("execute command - data process")
	print("------------------------------------------- running  " + command + " in ---- " + ec2_instance.public_dns_name )
	for line in data:
		x = line.decode()
		print(line.decode())
	print("------------------------------------------- running  " + command + " in ---- " + ec2_instance.public_dns_name )


def execute_script_in_ec2(ssh, ec2_instance, bucket, script):
	print ("execute_script_in_ec2 python " +  script + ".py")
	python_script = "python " + script + ".py"
	execute_script_in_ec2_generic(ssh, ec2_instance, bucket, python_script)


# download file from s3 bucket
def download_file_from_s3(ssh, ec2_instance, bucket, script):
	print ("download_file_from_s3 BUCKET_NAME ucd1")
	BUCKET_NAME = bucket # replace with your bucket name
	KEY = script + '.tar.gz' # replace with your object key
	gz_file = script + '.tar.gz'
	print(BUCKET_NAME)
	print("OBJECT KEY: " + KEY)  # dre1_test1.tar.gz
	print(gz_file)

	s3 = boto3.resource('s3')

	try:
		print("get file from S3")
		s3.Bucket(BUCKET_NAME).download_file(KEY, gz_file)
	except botocore.exceptions.ClientError as e:
		print("ERROR getting file from S3")
		if e.response['Error']['Code'] == "404":
			print("The object does not exist.")
		else:
			raise


def main():
	print (">>>>>>>>>>>>>>> STEP1 - create ec2 instance if it doesn't already exit")
	# createKey()

	EC2 = boto3.resource('ec2')

	print("get instances")
	instances = EC2.instances.filter(Filters=[{'Name': 'instance-state-name', 'Values': ['running']}])
	i = 0
	ec2_instance_id = "no_running_ec2_instances"
	ec2_instance = "no_running_ec2_instances"
	for instance in instances:
		print(instance.id, instance.instance_type)
		ec2_instance_id = instance.id
		ec2_instance = instance

	print("get running instance")
	print (ec2_instance_id)
	print (ec2_instance)

	if (ec2_instance_id == "no_running_ec2_instances"):
		print ("creating instance")
		ec2_instance = EC2.create_instances(ImageId='ami-00068cd7555f543d5', MinCount=1, MaxCount=1, KeyName="sft2020-x10-a", InstanceType='t3.micro')
		print ("new instance name : " + ec2_instance[0].id)
		print ("connect: " + ec2_instance_id)
	
	print(ec2_instance_id)
	print(ec2_instance)

	print (">>>>>>>>>>>>>>> STEP2 - Setup ssh")
	x = 1 
	# Setup ssh using paramiko
	ssh = paramiko.SSHClient()
	ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
	# for now we are using our key - but we can also create key (see routine to create key)
	privkey =  paramiko.RSAKey.from_private_key_file('../../../keys/sft2020-x10-a.pem')


	print (">>>>>>>>>>>>>>> STEP3 - Connect ec2 instance")
	try: 
		# now connect to ec2 instance
		print (ec2_instance)
		print ("connect " + ec2_instance.public_dns_name)
		ssh.connect(ec2_instance.public_dns_name,username='ec2-user',pkey=privkey)
		
		print (">>>>>>>>>>>>>>> STEP4 - clean environment")
		execute_script_in_ec2_generic(ssh, ec2_instance, "ucd1", "rm *.gz*")
		execute_script_in_ec2_generic(ssh, ec2_instance, "ucd1", "rm *.zip")
		
		print (">>>>>>>>>>>>>>> STEP5 - Deploy  test script")
		deploy_to_aws(ssh, ec2_instance, "ucd1", "dre1_test1", ".tar.gz")
		
		print (">>>>>>>>>>>>>>> STEP6 - Deploy  problem2 script")
		deploy_to_aws(ssh, ec2_instance, "ucd1", "problem2", ".tar.gz")
		
		print (">>>>>>>>>>>>>>> STEP7 - Deploy  EVENTS_10856_111111001 script")
		# https://ucd1.s3.amazonaws.com/EVENTS_10856_111111001.csv.zip
		deploy_to_aws(ssh, ec2_instance, "ucd1", "EVENTS_10856_111111001", ".csv.zip")

		# Setup environment for ec2 machine
		# sudo yum install python37
		# curl -O https://bootstrap.pypa.io/get-pip.py
		# python get-pip.py --user
		# python3 get-pip.py --user
		# pip install pyspark
		print (">>>>>>>>>>>>>>> STEP8 - execute test script")
		execute_script_in_ec2(ssh, ec2_instance, "ucd1",  "dre1_test1")
		print (">>>>>>>>>>>>>>> STEP9 - execute problem2 script")
		execute_script_in_ec2(ssh, ec2_instance, "ucd1",  "problem2")

		ssh.close()		
	except:
		print("----------------------------------------------------------------------------------")
		print("WARNING: If this is the first time - the aws may be initializing.  Try after 5 minutes")
		print("----------------------------------------------------------------------------------")


# main function of python
if __name__ == "__main__":
	print("----------------------------------------------------------------------------------")
	print ("__main__")
	main()
	print ("DONE")
	print("----------------------------------------------------------------------------------")



