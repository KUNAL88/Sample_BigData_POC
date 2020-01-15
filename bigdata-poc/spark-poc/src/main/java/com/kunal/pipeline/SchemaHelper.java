package com.kunal.pipeline;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;

public class SchemaHelper {

    private static final long serialversionUID=0l;
    private static final String[] partitionColumn={"server_date","partition_load_id"};

    private long inputTupleCount=0l;

    public SchemaHelper(){
        super();
    }

    private static final StructField[] Common_NetworkInfoType_struct={
      new StructField("mac",DataTypes.StringType,true,Metadata.empty()),
            new StructField("ipv4",DataTypes.createArrayType(DataTypes.StringType),true,Metadata.empty()),
            new StructField("ipv6",DataTypes.createArrayType(DataTypes.StringType),true,Metadata.empty()),
    };

    private static final StructField[] Common_signature_context_object_struct={
            new StructField("cert_chain",DataTypes.createArrayType(DataTypes.createStructType(cert)))
    };

    private static final StructField[] bashxdetectionfields={

       new StructField("server_ts", DataTypes.TimestampType,true, Metadata.empty()),
            new StructField("filename", DataTypes.StringType,true, Metadata.empty()),
            new StructField("seq_num", DataTypes.LongType,true, Metadata.empty()),

            new StructField("metadata__stic_version", DataTypes.StringType,true, Metadata.empty()),
            new StructField("metadata__stic_uid", DataTypes.StringType,true, Metadata.empty()),
            new StructField("metadata__stic_hw_uid", DataTypes.StringType,true, Metadata.empty()),
            new StructField("metadata__stic_schema_id", DataTypes.StringType,true, Metadata.empty()),
            new StructField("metadata__stic_cat", DataTypes.StringType,true, Metadata.empty()),
            new StructField("metadata__stic_legacy_uids", DataTypes.createArrayType(DataTypes.StringType),true, Metadata.empty()),
            new StructField("metadata__stic_legacy_hw_uids", DataTypes.createArrayType(DataTypes.StringType),true, Metadata.empty()),
            new StructField("metadata__stic_legacy_ent_uids", DataTypes.createArrayType(DataTypes.StringType),true, Metadata.empty()),
            new StructField("metadata__device_type", DataTypes.StringType,true, Metadata.empty()),
            new StructField("metadata__device_name", DataTypes.StringType,true, Metadata.empty()),
            new StructField("metadata__device_name_md5", DataTypes.StringType,true, Metadata.empty()),
            new StructField("metadata__device_domain", DataTypes.StringType,true, Metadata.empty()),
            new StructField("metadata__device_os_name", DataTypes.StringType,true, Metadata.empty()),
            new StructField("metadata__device_vhost", DataTypes.StringType,true, Metadata.empty()),
            new StructField("metadata__stic_has_pii", DataTypes.BooleanType,true, Metadata.empty()),
            new StructField("metadata__networks",DataTypes.createArrayType(DataTypes.createStructType(Common_NetworkInfoType_struct)),true, Metadata.empty()),
            new StructField("id", DataTypes.StringType,true, Metadata.empty()),
            new StructField("bash_timestamp", DataTypes.StringType,true, Metadata.empty()),
            new StructField("bash_engine_type", DataTypes.StringType,true, Metadata.empty()),
            new StructField("bash_engine_id", DataTypes.StringType,true, Metadata.empty()),
            new StructField("bash_confidence_level", DataTypes.StringType,true, Metadata.empty()),
            new StructField("detection_hid_level", DataTypes.IntegerType,true, Metadata.empty()),
            new StructField("product_hid_level", DataTypes.IntegerType,true, Metadata.empty()),
            new StructField("engine_info__platform_name", DataTypes.StringType,true, Metadata.empty()),
            new StructField("image_list__detection_info__file_signatures",DataTypes.createArrayType(DataTypes.createStructType(Common_signature_context_object_struct)),true,Metadata.empty()),
    };
}
