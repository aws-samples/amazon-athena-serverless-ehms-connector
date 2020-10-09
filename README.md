# AthenaLocalHiveMetastore
This project is an implementation of https://github.com/awslabs/aws-athena-hive-metastore. 
It uses Embedded Hive metastore client to connect to RDS Hive metastore directly from Athena/Lambda without the need for an EMR cluster or Thrift server. To make this approach truly serverless, it is highly recommended that an S3 location is chosen as Hive meta warehouse. 
The source code includes the reference project implementation code and it is a Maven project with the following modules.

*hms-service-api: the APIs between Lambda function and Athena service clients, which are defined in the HiveMetaStoreService interface. Since this is a service contract, please don’t change anything in this module. 

*hms-lambda-handler: a set of default lambda handlers to process each hive metastore API calls. The class MetadataHandler is the dispatcher for all different API calls. Customer don’t need to change this package either.

*hms-lambda-layer: a Maven assembly project to put hms-sevice-api, hms-lambda-handler, and their dependencies into a zip file so that this zip file could be registered as a Lambda layer and then could be used by multiple Lambda functions.

*hms-lambda-func: *an example Lambda function, where
HiveMetaStoreLambdaFunc: the example lambda function and it simply extends MetadataHandler.
EmbeddedHiveMetaStoreClientFactory: controls the behavior of the lambda function, for example, customer could provide their own set of HandlerProviders by overriding the getHandlerProvider() method.
hms.properties: Lambda function configuration. Connection parameters can either be defined in hms.properties or in the Lambda as environment variables. An example property in this file:
hive.metastore.response.spill.location: the s3 location to store response objects when their sizes exceed a given threshold, for example, 4MB. The threshold is defined in the property “hive.metastore.response.spill.threshold”, but we don’t recommend customer change the default value.
The two properties could be overridden by Lambda environment variables (https://docs.aws.amazon.com/lambda/latest/dg/env_variables.html) so that customer don’t need to recompile the source code for different Lambda functions with different properties.

The artifacts consists of the following files

hms-lambda-func-1.0-withdep.jar: an example Lambda function with all runtime dependencies, this jar can be used alone to define a lambda function
hms-lambda-layer-1.0-athena.zip: the runtime library for Lambda functions as a Lambda layer (https://docs.aws.amazon.com/lambda/latest/dg/configuration-layers.html).
hms-lambda-func-1.0.jar: an example lightweight Lambda function and it relies on the layer to provide Lambda runtime dependencies

To run the standalone JAR with dependencies from Athena:

1) mvn clean package  
2) Create a Lambda function with the JAR hms-lambda-func/target/hms-lambda-func-1.0-SNAPSHOT-withdep.jar. Please make sure to allocate 3008 MB memory (upper most limit) for Lambda as this approach is memory intensive compared to Thrift approach due to Hive metastore client initiation within Lambda itself
3) From Lambda, define the following environment variables with KMS encryption enabled both at-rest and in-transit. Please use S3A protocol to specify warehouse location

```
CONNECTION_URL - jdbc:mysql://your-rds-endpoint:3306/dbName?trustServerCertificate=true&useSSL=true&requireSSL=true&verifyServerCertificate=false
DRIVER_NAME - org.mariadb.jdbc.Driver
PASSWORD - password  
SPILL LOCATION - s3://my-hms/lambda/functions/spill
USER_NAME - username
WAREHOUSE_LOCATION - s3a://my-hms-warehouse-location/prefixName
```

Alternatively, following properties can be specified in hms-lambda-func/src/main/resources/hms.properties before the maven build i.e., step 1

```
javax.jdo.option.ConnectionURL=jdbc:mysql://your-rds-endpoint:3306/dbName?trustServerCertificate=true&useSSL=true&requireSSL=true&verifyServerCertificate=false
javax.jdo.option.ConnectionDriverName=org.mariadb.jdbc.Driver
javax.jdo.option.ConnectionPassword=changeit
javax.jdo.option.ConnectionUserName=changeit
hive.metastore.warehouse.dir=s3a://my-hms-warehouse-location/prefixName
hive.metastore.response.spill.location=s3://my-hms/lambda/functions/spill
```

4) Create a Hive Metastore data connector and provide this Lambda function 
