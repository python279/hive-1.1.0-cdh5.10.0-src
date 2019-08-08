#!/bin/sh

source /opt/jdk7.sh
export PATH=/opt/apache-maven-3.6.0/bin:$PATH

mvn clean package -DskipTests -Phadoop-2 -Pdist
