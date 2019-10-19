SET mapreduce.map.memory.mb 1024;
SET mapreduce.reduce.memory.mb 512;
SET mapreduce.map.java.opts -Xmx812m;
SET mapreduce.reduce.java.opts -Xmx312m;

--SET tez.am.resource.memory.mb 1024;
--SET tez.runtime.io.sort.mb 512;

address_book_raw_pings = LOAD '${inputDir}' USING com.kunal.proto.pig.AddressBookLoader(
                        'addressbook_bag:bag{T:tuple(src_filename:chararray,person_id:int,person_name:chararray,person_email:chararray,mobile_num:long)}'
                        );

address_book_final_ping = FOREACH address_book_raw_pings GENERATE
                        FLATTEN(addressbook_bag);

STORE address_book_final_ping INTO '${outputDir}/addressbook_stage' USING OrcStorage();