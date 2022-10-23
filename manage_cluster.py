import pandas as pd
import boto3
import json
import configparser
import boto3
import time

from botocore.exceptions import ClientError

# Load parameter from dwh.cfg file:

config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')

DWH_CLUSTER_TYPE       = config.get("CLUSTER","DWH_CLUSTER_TYPE")
DWH_NUM_NODES          = config.get("CLUSTER","DWH_NUM_NODES")
DWH_NODE_TYPE          = config.get("CLUSTER","DWH_NODE_TYPE")
DWH_CLUSTER_IDENTIFIER = config.get("CLUSTER","DWH_CLUSTER_IDENTIFIER")
DWH_REGION             = config['CLUSTER']['DWH_REGION']
DWH_IAM_ROLE_NAME      = config.get("CLUSTER", "DWH_IAM_ROLE_NAME")

DWH_DB                 = config.get("DB","DWH_DB")
DWH_DB_USER            = config.get("DB","DWH_DB_USER")
DWH_DB_PASSWORD        = config.get("DB","DWH_DB_PASSWORD")
DWH_PORT               = config.get("DB","DWH_PORT")

S3_READ_ARN = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"


def create_resources():
    """
    Create clients for ec2, s3, iam and redshift
    """
    
    ec2 = boto3.resource('ec2',
                       region_name=DWH_REGION,
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
    )
    s3 = boto3.resource('s3',
                       region_name=DWH_REGION,
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
    )
    iam = boto3.client('iam',aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET,
                     region_name=DWH_REGION
    )
    redshift = boto3.client('redshift',
                       region_name=DWH_REGION,
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
    )
    return ec2, s3, iam, redshift

def create_iam_role(iam):
    """
    Create IAM role for Redshift cluster
    """
    
    try:
        print("1.1 Creating a new IAM Role") 
        dwhRole = iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{
                    'Action': 'sts:AssumeRole',
                    'Effect': 'Allow',
                    'Principal': {'Service': 'redshift.amazonaws.com'}
                }],
                'Version': '2012-10-17'
            })
        )
        print("1.2 Attaching Policy")
        iam.attach_role_policy(
            RoleName=DWH_IAM_ROLE_NAME,
            PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
        )['ResponseMetadata']['HTTPStatusCode']
    except Exception as e:
        print(e)
        
    print("1.3 Get the IAM role ARN")
    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
    return roleArn
    
def create_redshift_cluster(redshift, roleArn):
    """
    Create Reashift Cluster
    """
    
    try:
        response = redshift.create_cluster(        
            #HW
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),

            #Identifiers & Credentials
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,

            #Roles (for s3 access)
            IamRoles=[roleArn]
        )    
        timestep = 15
        for _ in range(int(600/timestep)):
            cluster = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
            if cluster['ClusterStatus'] == 'available':
                print('Cluster {} is AVAILABLE'.format(DWH_CLUSTER_IDENTIFIER))
                break
            print('Cluster status is "{}". Please wait. Retrying in {} seconds.'.format(cluster['ClusterStatus'], timestep))
            time.sleep(timestep)

    except Exception as e:
        print(e)
        
def get_endpoint_arn(redshift):
    """
    Get and print Redshift Cluster Endpoint and ARN
    """
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
    DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
    print("DWH_ENDPOINT :: ", DWH_ENDPOINT)
    print("DWH_ROLE_ARN :: ", DWH_ROLE_ARN)
    return myClusterProps
        
def open_tcp_port(ec2, VpcId):
    """
    Open a incoming TCP port
    """
    
    try:
        vpc = ec2.Vpc(id=VpcId)
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT)
        )
    except Exception as e:
        print(e)
        
def delete_redshift_cluster(redshift):
    """
    Delete Redshift Cluster
    """
    deleting = False
    try:
        redshift.delete_cluster(
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            SkipFinalClusterSnapshot=True,
        )
        cluster = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
        if cluster['ClusterStatus'] == 'deleting':
            deleting = True
        timestep = 15   
        while cluster['ClusterStatus'] == 'deleting':
            cluster = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
            print('Cluster status is "{}". Please wait. Retrying in {} seconds.'.format(cluster['ClusterStatus'], timestep))
            time.sleep(timestep)

    except Exception as e:
        if deleting:
            print("Cluster has been DELETED")
        else:
            print(e)
            
def delete_resourses(iam, roleArn):  
    """
    Delete clients for ec2, s3, iam and redshift
    """
    try:
        role_arn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
        iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn=S3_READ_ARN)
        iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)
        print('Deleted role {} with {}'.format(DWH_IAM_ROLE_NAME, roleArn))
    except Exception as e:
        print(e)

def input_choice():
    """
    Choose between create or delete cluster
    """
    choice = ""
    while True:
        choice = input("Type 'c' to create the cluster or 'd' to delete the cluster: ")
        if(choice.lower() == 'c' or choice.lower() == 'd'):
            break
        else:
            print("Invalid choice!")
    return choice

def main():
    ec2, s3, iam, redshift = create_resources()
    roleArn = create_iam_role(iam)
    choice = input_choice()
    if choice == 'c':
        print(roleArn)
        create_redshift_cluster(redshift, roleArn)
        myClusterProps = get_endpoint_arn(redshift)
        open_tcp_port(ec2, myClusterProps['VpcId'])
    else: 
        delete_redshift_cluster(redshift)
        delete_resourses(iam, roleArn)

if __name__ == "__main__":
    main()