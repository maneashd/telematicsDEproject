Telematics Data Engineering Project

![Project Architecture](https://github.com/maneashd/telematicsDEproject/blob/main/img/car_telematics_arch.png)

## 📌 Overview
This project demonstrates an end-to-end telematics data pipeline that simulates vehicle movement data, processes it in real-time, and stores it in a data lake with Iceberg table format for efficient querying. The pipeline integrates multiple AWS services and Snowflake for a complete data solution.

## 🚀 Key Features
- Real-time vehicle data simulation
- Kafka-based event streaming
- Spark Structured Streaming for data processing
- AWS S3 as data lake storage
- Iceberg table format for ACID transactions
- AWS Glue for ETL and catalog management
- Snowflake integration for analytics

## 🛠️ Technology Stack
- **Data Generation**: Python, Confluent Kafka
- **Stream Processing**: PySpark, Spark Structured Streaming
- **Storage**: AWS S3, Apache Iceberg
- **ETL & Catalog**: AWS Glue
- **Data Warehouse**: Snowflake
- **Infrastructure**: Docker, AWS

## 🚗 Data Flow
1. **Data Generation**: Simulated vehicle data is produced to Kafka topics
2. **Stream Processing**: Spark consumes Kafka topics and writes to S3
3. **ETL Processing**: Glue job transforms data into Iceberg format
4. **Data Catalog**: Glue Data Catalog manages metadata
5. **Data Warehouse**: Snowflake accesses Iceberg tables for analytics


## Data Creation
The idea is to capture a vehicle realtime data travelliing from point A to point B. This data is create using python functions written in main.py.\
The important function involved in publishing data to kafka topic is:
```python
def simulate_journey(producer, device_id):
    while True:
        vehicle_data = generate_vehicle_data(device_id)                    # Dummy data is being generated.
        gps_data = generate_gps_data(device_id, vehicle_data['timestamp'])
        traffic_camera_data = generate_traffic_camera_data(device_id, vehicle_data['timestamp'],
                                                           vehicle_data['location'], 'Nikon-Cam123')
        weather_data = generate_weather_data(device_id, vehicle_data['timestamp'], vehicle_data['location'])
        emergency_incident_data = generate_emergency_incident_data(device_id, vehicle_data['timestamp'],
                                                                   vehicle_data['location'])

        if (vehicle_data['location'][0] >= BIRMINGHAM_COORDINATES['latitude']
                and vehicle_data['location'][1] <= BIRMINGHAM_COORDINATES['longitude']):
            print('Vehicle has reached Birmingham. Simulation ending...')
            break

        produce_data_to_kafka(producer, VEHICLE_TOPIC, vehicle_data)        #data is being published to VEHICLE_TOPIC.
        produce_data_to_kafka(producer, GPS_TOPIC, gps_data)
        produce_data_to_kafka(producer, TRAFFIC_TOPIC, traffic_camera_data)
        produce_data_to_kafka(producer, WEATHER_TOPIC, weather_data)
        produce_data_to_kafka(producer, EMERGENCY_TOPIC, emergency_incident_data)

        time.sleep(3)
```

## ETL Process
### 1. Docker Setup
As the first step I installed docker desktop and executed docker-compose.yml file to spin up containers required to execute this project.\
which include
- Kafka Server
- Zookeeper
- Spark-master
- Spark-worker1
- sparl-worker2

![Data Extraction](https://github.com/maneashd/telematicsDEproject/blob/main/img/docker.png)

### 1. Data Extraction
- Step 1
  ```python
  spark = SparkSession.builder.appName("SmartCityStreaming")
  ```
  A spark session is created using SparkSession builder

- Step 2
  For each kafka topic a schema is created mainly used to subscribe the data published.
  ```pyton
  vehicleSchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("speed", DoubleType(), True),
        StructField("direction", StringType(), True),
        StructField("make", StringType(), True),
        StructField("model", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("fuelType", StringType(), True),
    ])
  ```
### 2. Data Transformation
  A function reads the kafka topic and creates a SparkDataframe when called.
  ```python
          def read_kafka_topic(topic, schema):
            return (spark.readStream
                    .format('kafka')
                    .option('kafka.bootstrap.servers', 'broker:29092')
                    .option('subscribe', topic)
                    .option('startingOffsets', 'earliest')
                    .load()
                    .selectExpr('CAST(value AS STRING)')
                    .select(from_json(col('value'), schema).alias('data'))
                    .select('data.*')
                    .withWatermark('timestamp', '2 minutes')
                    )

  vehicleDF = read_kafka_topic('vehicle_data', vehicleSchema).alias('vehicle')        # Here 'vehicle_data' is the kafka topic from which data is read.
  ```

### 3. Data Loading
- Step 1
  Create AWS S3 Bucket
    ![Data Transformation](https://github.com/maneashd/telematicsDEproject/blob/main/img/S3.png)
    S3 bucket is created and required permissions are given for the spark aplication to write its data to S3. Here the data is converted to parquet format.
- Step 2
  ```python
      def streamWriter(input: DataFrame, checkpointFolder, output):
        return (input.writeStream
                .format('parquet')
                .option('checkpointLocation', checkpointFolder)
                .option('path', output)
                .outputMode('append')
                .start())
  ```
  This function takes Dataframe as input and reads the schematic data and writes it to AWS S3 bucket provided as argument.
  ```python
  query1 = streamWriter(vehicleDF, 's3a://spark-streaming-data01/checkpoints/vehicle_data',
                 's3a://spark-streaming-data01/data/vehicle_data')
  ```
- Step 3
  The data stored in S3 will be read by AWS Glue job which will infer schema and create a data catalog which can be made availabel for snowflake.
![Data Catalog using Glue And Apache Iceberg]

- Step 4
  - Step to create icebergtables and glue tables.
  1) Create a data base telematics
  2) Open Athena and create a table under the data base pointing to S3 data that was sent by vehilce
     ```sql
        CREATE EXTERNAL TABLE awsdatacatalog.telematics_db.vehicle_schema (
          id STRING,
          deviceId STRING,
          timestamp TIMESTAMP,  
          location STRING,
          speed DOUBLE,
          direction STRING,
          make STRING,
          model STRING,
          year INTEGER,
          fuelType STRING
        )
        ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
        WITH SERDEPROPERTIES (
          'ignore.malformed.json' = 'true'
        )
        STORED AS TEXTFILE
        LOCATION 's3://spark-straming-data02/data/vehicle_data/'
        TBLPROPERTIES (
          'classification' = 'json',
          'has_encrypted_data' = 'false'
        );
     ```
     This is a athena table for raw data general purpose is to gandle the raw schema and point to the raw data location.
  4) Create a glue job using spark to read the s3 data and infer the schema
  ```python
  # Read all files in vehicle_data folder efficiently
    vehicle_raw_df = spark.read \
        .option("mode", "PERMISSIVE") \
        .option("inferSchema", "true") \
        .json("s3://spark-straming-data02/data/vehicle_data/part-*.json") \
        .withColumn("file_name", input_file_name())
  ```
  5) Then spark job itself creates an iceberg table in glue and writes the s3 data in iceberg format to the s3_iceberg folders and also the metadat to glue_iceberg table.
  ```python
    # Write to Iceberg table with partitioning
    vehicle_raw_df.write \
        .format("iceberg") \
        .mode("overwrite") \
        .option("write.format.default", "parquet") \
        .saveAsTable(f"{catalog_nm}.{database_op}.{table_op}")
  ```

![Integrate Snowflake to Glue and S3]
- Steps
  1) Create a Snowflake account on aws.
  2) Create an External Volume in SF.
     ```sql
     CREATE OR REPLACE EXTERNAL VOLUME iceberg_ex_vol
      STORAGE_LOCATIONS = (
        (
          NAME = 'ice-berg-s3-location'
          STORAGE_PROVIDER = 'S3'
          STORAGE_BASE_URL = 's3://spark-straming-data02/iceberg_data/'
          STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::********:role/GlueS3FullAccessRole'
        )
      );
     ```
  4) Create glue_integration in SF.
     ```sql
        CREATE OR REPLACE CATALOG INTEGRATION glue_catalog_int
          CATALOG_SOURCE = GLUE
          CATALOG_NAMESPACE = 'telematics_db'
          TABLE_FORMAT = ICEBERG
          GLUE_AWS_ROLE_ARN = 'arn:aws:iam::************:role/GlueS3FullAccessRole'
          GLUE_CATALOG_ID = '390844770355'
          GLUE_REGION = 'us-east-1'
          ENABLED = TRUE;
     ```
  6) Create Iceberg table to use the ACID propertice and time travel properties of the iceberg formatted data in s3.
     ```sql
     CREATE OR REPLACE ICEBERG TABLE vehicledata_ice
        EXTERNAL_VOLUME = 'iceberg_ex_vol'
        CATALOG = 'glue_catalog_int'
        CATALOG_TABLE_NAME = 'vehicle_schema_iceb';
     ```
## Usage
To run the ETL process, follow these steps:
1. Clone the repository.
2. Set up the necessary dependencies.
3. Execute the extraction, transformation, and loading scripts in sequence.

## Conclusion
This README provides an overview of the ETL project, including its architecture, technologies used, and the ETL process. By following the provided instructions, users can run the ETL process and analyze the transformed data.

Feel free to customize this template according to your project's specific requirements and add additional sections as needed.
