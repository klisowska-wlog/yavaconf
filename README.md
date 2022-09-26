# yavaconf
Yava!Conf conference GitHub repository with Java code for Kafka clients

### Installation
In order for the apps to work properly we need an active Zookeeper instance and at least 1 Kafka broker available.
If Docker is available then the docker-compose.yml file attached to this project can be used to quickly get it running.
It's sufficient to run the following command from the project source directory (yavaconf):

``docker-compose up``

This will start a Zookeeper instance available at port _2182_ and 1 Kafka broker listening on port _9092_.

###Useful Kafka CLI commands
`kafka-topics.sh --list --zookeeper localhost:2182`
`kafka-broker-api-versions.sh --bootstrap-server=:9092 | grep 9092`