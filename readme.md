Spark SQS Receiver
==================
SQS Amazon queue receiver for the Spark, example usage:

```java
    jssc.receiverStream(new SQSReceiver("sqs-queue-name")
                        .withCredentials("key","secret")
                        .at(Regions.US_EAST_1)
    );
```

   or

```java
    jssc.receiverStream(new SQSReceiver("sqs-queue-name")
                            .withCredentials("aws properties file with credentials")
                            .at(Regions.US_EAST_1)
    );
```

   or

```java
    jssc.receiverStream(new SQSReceiver("sqs-queue-name"));
```

Where:
* name ("sample" in the example above) - name of the queue
* credentials - AWS credentials
* region - region where the queue exists

By default credentials are empty (DefaultAWSCredentialsProviderChain), regions is Regions.DEFAULT_REGION