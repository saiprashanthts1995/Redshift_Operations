# Redshift_Operations
Perform Various Operations in Redshift like 
* Load data into the table present in Redshift
* Run copy command in Redshift
* Create External table in Redshift

## Purpose of this Project
1. Learn how to spin up redshift cluster
2. Load fixed width data into redshift by creating an table
3. Run copy command to copy the data present in s3 into the table
4. Create external schema and table to load the file present in the AWS S3

## Tools Used to create the Project
1. Pycharm for coding
2. DBeaver for writing queries
3. WinScp to SSH to EC2 Instance
4. putty to run the scripts in AWS Ec2 Instance
5. Database hosted in AWS Redshift

## Command to run the script
1. To run fixed width section

    ```python3 app.py --section FIXED_WIDTH --table_name Departments ```
2. To execute Copy Section

    ```python3 app.py --section COPY --table_name Customers ```
3. To execute External table Section

    ```python3 app.py --section CREATE_EXTERNAL --table_name Iris ```

## Contents 



## Images/ Screenshot
- EC2 Instance Security Group


- Data in Redshift


- Loading data from Ec2 Instances


- Files present in S3  



## Author
Name : Sai Prashanth T S

Email : saiprashanthts@gmail.com
