#!/usr/bin/env bash

destIp=ec2-13-126-212-129.ap-south-1.compute.amazonaws.com

rm -rf build-files && \
mvn clean package -DskipTests=true && \
echo "Completed packaging, deploying to $destIp" && \
scp -r build-files/* ec2-user@$destIp:codeDrops/ && \
echo "Deployed in VM $destIp"

exit 0



scp demo-data/omniture-raw/* cloudera@192.168.56.101:dataDrops/
scp demo-data/sales-raw/* cloudera@192.168.56.101:dataDrops/


cd codeDrops/local
sh local/05_IngestData.sh  ~/dataDrops/Omniture.0.tsv.gz ~/dataDrops/online-retail-sales-data.csv

