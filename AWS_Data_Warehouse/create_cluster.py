# parsing S3 files:
# https://stackoverflow.com/questions/39275043/parse-files-in-aws-s3-with-boto3

import pandas as pd
import boto3
import json
import configparser
import psycopg2

def createIAMrole(iam, DWH_IAM_ROLE_NAME):
    # 1.1 Create IAM role,
    try:
        print("Creating a new IAM Role")
        dwhRole = iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description="Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                                'Effect': 'Allow',
                                'Principal': {'Service': 'redshift.amazonaws.com'}}],
                 'Version': '2012-10-17'})
        )
    except Exception as e:
        print(e)

    # 1.2 Attach policy
    print("1.2 Attaching Policy")

    iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                           PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                           )['ResponseMetadata']['HTTPStatusCode']

    print("1.3 Get the IAM role ARN")
    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']

    print(roleArn)
    return roleArn

def createRedshiftCluster(redshift,roleArn, DWH_CLUSTER_TYPE, DWH_NODE_TYPE, DWH_NUM_NODES, DWH_DB, DWH_CLUSTER_IDENTIFIER, DWH_DB_USER, DWH_DB_PASSWORD):
    # Create a RedShift Cluster
    try:
        response = redshift.create_cluster(
            # HW
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),

            # Identifiers & Credentials
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,

            # Roles (for s3 access)
            IamRoles=[roleArn]
    )
    except Exception as e:
        print(e)

def checkCusterStatus(redshift,DWH_CLUSTER_IDENTIFIER):
    # Describe the cluster to see its status
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint",
                  "NumberOfNodes", 'VpcId']
    x = [(k, v) for k, v in myClusterProps.items() if k in keysToShow]
    pd.DataFrame(data=x, columns=["Key", "Value"])

    return myClusterProps

def openIncomingTCPport(ec2,VpcId,DWH_PORT):
    # Open an incoming  TCP port to access the cluster endpoint
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

def main():
    import sys
    print(sys.executable)
    # Load DWH Params from config file
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    KEY = config.get('AWS', 'KEY')
    SECRET = config.get('AWS', 'SECRET')

    DWH_CLUSTER_TYPE = config.get("DWH", "DWH_CLUSTER_TYPE")
    DWH_NUM_NODES = config.get("DWH", "DWH_NUM_NODES")
    DWH_NODE_TYPE = config.get("DWH", "DWH_NODE_TYPE")

    DWH_CLUSTER_IDENTIFIER = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")
    DWH_DB = config.get("DWH", "DWH_DB")
    DWH_DB_USER = config.get("DWH", "DWH_DB_USER")
    DWH_DB_PASSWORD = config.get("DWH", "DWH_DB_PASSWORD")
    DWH_PORT = config.get("DWH", "DWH_PORT")

    DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")

    pd.DataFrame({"Param":
                      ["DWH_CLUSTER_TYPE", "DWH_NUM_NODES", "DWH_NODE_TYPE", "DWH_CLUSTER_IDENTIFIER", "DWH_DB",
                       "DWH_DB_USER", "DWH_DB_PASSWORD", "DWH_PORT", "DWH_IAM_ROLE_NAME"],
                  "Value":
                      [DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER,
                       DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME]
                  })

    # Create clients for IAM, EC2, S3 and Redshift
    ec2 = boto3.resource('ec2',
                         region_name="us-west-2",
                         aws_access_key_id=KEY,
                         aws_secret_access_key=SECRET
                         )

    s3 = boto3.resource('s3',
                        region_name="us-west-2",
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                        )

    iam = boto3.client('iam', aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET,
                       region_name='us-west-2'
                       )

    redshift = boto3.client('redshift',
                            region_name="us-west-2",
                            aws_access_key_id=KEY,
                            aws_secret_access_key=SECRET
                            )
    # STEP1: Create an IAM Role that makes Redshift able to access S3 bucket (ReadOnly)
    roleArn = createIAMrole(iam,DWH_IAM_ROLE_NAME)

    # STEP2: Create a RedShift Cluster
    createRedshiftCluster(redshift,roleArn,DWH_CLUSTER_TYPE, DWH_NODE_TYPE, DWH_NUM_NODES, DWH_DB, DWH_CLUSTER_IDENTIFIER, DWH_DB_USER, DWH_DB_PASSWORD)

    # Check cluster status
    myClusterProps = checkCusterStatus(redshift, DWH_CLUSTER_IDENTIFIER)

    DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
    DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
    print("DWH_ENDPOINT :: ", DWH_ENDPOINT)
    print("DWH_ROLE_ARN :: ", DWH_ROLE_ARN)

    # STEP3: Open an incoming  TCP port to access the cluster endpoint
    openIncomingTCPport(ec2, myClusterProps['VpcId'],DWH_PORT)

    # STEP 4: Make sure you can connect to the cluster
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    conn.close()

if __name__ == "__main__":
    main()