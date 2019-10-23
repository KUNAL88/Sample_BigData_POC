CREATE DATABASE IF NOT EXISTS ${database};

USE ${database};

set hadoop.security.credential.provider.path=${s3_credential_file_rw};

CREATE EXTERNAL TABLE IF NOT EXISTS ${employee_hot_table_name}(
id int,
firstname string,
lastname string,
email_id string
)
PARTITIONED BY (server_date DATE, load_inst_id BIGINT)
STORED AS ORC
LOCATION '${employee_hot_table_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY");

