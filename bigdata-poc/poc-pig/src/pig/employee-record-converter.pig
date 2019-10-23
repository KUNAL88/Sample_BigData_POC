SET mapreduce.map.memory.mb 1024;
SET mapreduce.reduce.memory.mb 512;
SET mapreduce.map.java.opts -Xmx812m;
SET mapreduce.reduce.java.opts -Xmx312m;

employee_raw_pings = LOAD '${inputDir}' USING com.kunal.json.pig.EmployeeLoader(
                        'employee_bag:bag{T:tuple(id:int,name:chararray,lastname:chararray,email:chararray)}'
                        );

employee_final_ping = FOREACH employee_raw_pings GENERATE
                        FLATTEN(employee_bag);

STORE employee_final_ping INTO '${outputDir}/employee_stage' USING OrcStorage();

