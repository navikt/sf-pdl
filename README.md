# sf pdl 

Imports relevant persons from kafka pdl compaction log and filter process relevant data to a Salesforce person compaction log topic.
This topic is used as a cach to only process new and updated data. Consumer of this topic is the application sf-person.

## Tools
- Kotlin
- Gradle
- Kotest test framework

## Components
- Kafka client
- Protobuf 3.0
- Http4k

## Build
```
./gradlew clean build installDist
```

## Contact us
[Björn Hägglund](mailto://bjorn.hagglund@nav.no)
[Torstein Nesby](mailto://torstein.nesby@nav.no)

For internal resources, send request/questions to slack #crm-plattform-team 