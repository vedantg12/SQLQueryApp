import boto3
import pymysql

s3_cient = boto3.client('s3')

# Read CSV file content from S3 bucket
def read_data_from_s3(event):
    bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
    s3_file_name = event["Records"][0]["s3"]["object"]["key"]
    resp = s3_cient.get_object(Bucket=bucket_name, Key=s3_file_name)

    data = resp['Body'].read().decode('utf-8')
    data = data.split("\n")
    return data

def lambda_handler(event, context):
    rds_endpoint  = ""
    username = "admin"
    password = "" # RDS Mysql password
    db_name = "" # RDS MySQL DB name
    conn = None
    try:
        conn = pymysql.connect(rds_endpoint, user=username, passwd=password, db=db_name, connect_timeout=5)
    except pymysql.MySQLError as e:
        print("ERROR: Unexpected error: Could not connect to MySQL instance.")

    try:
        cur = conn.cursor()
        cur.execute("create table Employees ( id INT NOT NULL AUTO_INCREMENT, Name varchar(255) NOT NULL, PRIMARY KEY (id))")
        conn.commit()
    except:
        pass

    data = read_data_from_s3(event)

    with conn.cursor() as cur:
        for emp in data: # Iterate over S3 csv file content and insert into MySQL database
            try:
                emp = emp.replace("\n","").split(",")
                print (">>>>>>>"+str(emp))
                cur.execute('insert into Employees (Name) values("'+str(emp[1])+'")')
                conn.commit()
            except:
                continue
        cur.execute("select count(*) from Employees")
        print ("Total records on DB :"+str(cur.fetchall()[0]))
        # Display employee table records
        # for row in cur:
        #     print (row)
    if conn:
        conn.commit()
