#!/usr/bin/env bash

cd scap2rdf
java -jar scap2rdf-0.0.2.jar -i /scap2rdf/arf.xml -o /scap2rdf/arf.rdf -ow
cd ..