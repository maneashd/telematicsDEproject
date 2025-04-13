CREATE DATABASE telematics;
USE telematics;


CREATE OR REPLACE EXTERNAL VOLUME iceberg_ex_vol
  STORAGE_LOCATIONS = (
    (
      NAME = 'ice-berg-s3-location'
      STORAGE_PROVIDER = 'S3'
      STORAGE_BASE_URL = 's3://spark-straming-data02/iceberg_data/'
      STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::390844770355:role/GlueS3FullAccessRole'
    )
  );


DESCRIBE EXTERNAL VOLUME iceberg_ex_vol;

CREATE OR REPLACE CATALOG INTEGRATION glue_catalog_int
  CATALOG_SOURCE = GLUE
  CATALOG_NAMESPACE = 'telematics_db'
  TABLE_FORMAT = ICEBERG
  GLUE_AWS_ROLE_ARN = 'arn:aws:iam::390844770355:role/GlueS3FullAccessRole'
  GLUE_CATALOG_ID = '390844770355'
  GLUE_REGION = 'us-east-1'
  ENABLED = TRUE;

DESCRIBE CATALOG INTEGRATION glue_catalog_int;

CREATE OR REPLACE ICEBERG TABLE vehicledata_ice
EXTERNAL_VOLUME = 'iceberg_ex_vol'
CATALOG = 'glue_catalog_int'
CATALOG_TABLE_NAME = 'vehicle_schema_iceb';


SELECT * FROM VEHICLEDATA_ICE;