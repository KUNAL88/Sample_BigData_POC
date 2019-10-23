USE ${database};
SET hive.support.concurrency=false;
SET hive.vectorized.execution.enabled=false;
SET hive.vectorized.execution.reduce.enabled=false;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.dynamic.partition=true;
SET hive.stats.autogather=true;
SET hive.exec.parallel=true;
SET hive.execution.engine=${hiveExecEngine};
SET mapred.job.queue.name=${queueName};
SET mapreduce.job.queue.name=${queueName};
SET tez.queue.name=${queueName};

SET mapreduce.map.memory.mb=512;
SET mapreduce.reduce.memory.mb=256;
SET mapreduce.map.java.opts=-Xmx256m;
SET mapreduce.reduce.java.opts=-Xmx1268m;

SET mapreduce.input.fileinputformat.split.maxsize=134217728;

SET mapreduce.job.reduce=6;
SET mapred.reduce.tasks=6;

SET hadoop.security.credential.provider.path=${s3_credential_file_rw};

--ADD jar ${libPath}/hive-commons-1.0.0.jar;

INSERT OVERWRITE TABLE ${employee_hot_table_name} partition (server_date,load_inst_id)
SELECT
id,
first_name,
last_name,
email_id,
TO_DATE(server_ts) AS server_date,
${current_time} as load_inst_id
FROM ${employee_stage_table_name}
CLUSTER BY (id);