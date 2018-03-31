# Kafka Streams, GDPR and Event Sourcing

This code is the companion of the blog post [Kafka, GDPR and Event Sourcing](http://danlebrero.com/)

This project uses Docker to create a cluster of 3 microservices that consume from a Kafka topic using the
Kafka Streams API.

## Usage

Docker should be installed.

To run:

     docker-compose up -d && docker-compose logs -f our-service our-service2 our-service3
     
Once the environment has been started, you have to add some data:

     curl --data "client=id1&name=Dan" -X POST http://localhost:3004/set-data
     curl --data "client=id1&name=Lebrero" -X POST http://localhost:3004/set-data

You will see in the logs that the event-consumer logs something like:

    [KSTREAM-REDUCE-0000000004]: id1, (Dan,Lebrero<-null)
    
To exercise the "right to erasure":
 
     curl --data "client=id1" -X POST http://localhost:3004/forget-me

Which should result on the event-consummer logging:

     [KSTREAM-REDUCE-0000000004]: id1, (forget-me!<-null)
         
     
## Clean up

To get rid of all:

    docker-compose down --rmi all --remove-orphans
    docker image rm pandeiro/lein:2.5.2