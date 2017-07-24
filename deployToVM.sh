#!/usr/bin/env bash

destIp=192.168.56.101

rm -rf build-files && \
mvn clean package -DskipTests=true && \
echo "Completed packaging, deploying to $destIp" && \
scp -r build-files/* cloudera@$destIp:codeDrops/ && \
echo "Deployed in VM $destIp"

exit 0



scp demo-data/omniture-raw/* cloudera@192.168.56.101:dataDrops/
scp demo-data/sales-raw/* cloudera@192.168.56.101:dataDrops/


cd codeDrops/local
sh local/05_IngestData.sh  ~/dataDrops/Omniture.0.tsv.gz ~/dataDrops/online-retail-sales-data.csv

