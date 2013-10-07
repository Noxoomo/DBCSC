#!/bin/sh
wget http://data.europeana.eu/download/2.0/datasets/nt/08515_Ag_EU_ATHENA_ChouvashiaStateArtMuseum.nt.gz
gzip -d 08515_Ag_EU_ATHENA_ChouvashiaStateArtMuseum.nt.gz
rm -r /tmp/test
mv 08515_Ag_EU_ATHENA_ChouvashiaStateArtMuseum.nt  data
scala createTest.scala
cd ../..
SBT_OPTS="-Xms512M -Xmx1024M -Xss1M -XX:+CMSClassUnloadingEnabled -XX:MaxPermSize=256M" sbt  'runMain client.StartDatabase  /tmp/test' < src/test/stress-test
