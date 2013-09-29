#!/bin/sh
wget http://data.europeana.eu/download/2.0/datasets/nt/08515_Ag_EU_ATHENA_ChouvashiaStateArtMuseum.nt.gz
gzip -d 08515_Ag_EU_ATHENA_ChouvashiaStateArtMuseum.nt.gz
mv 08515_Ag_EU_ATHENA_ChouvashiaStateArtMuseum.nt  data
scala createTest.scala
java -jar -Xmx1g dbcsc_2.10-0.01-one-jar.jar test < stress-test
