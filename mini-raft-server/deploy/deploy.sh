#!/usr/bin/env zsh

cd ../.. && mvn clean package -DskipTests
cd -

SERVER_JAR=mini-raft-server-1.0.0.jar
ROOT_DIR=./env
rm -r $ROOT_DIR
mkdir -p $ROOT_DIR
cd $ROOT_DIR

mkdir dev1
cd dev1
mkdir classpath
cp ../../../target/$SERVER_JAR .
cp ../../../target/classes/application-dev1.yml ./classpath/
nohup java -jar ./$SERVER_JAR --spring.config.location=classpath:/application-dev1.yml &
cd -

mkdir dev2
cd dev2
mkdir classpath
cp ../../../target/$SERVER_JAR .
cp ../../../target/classes/application-dev2.yml ./classpath/
nohup java -jar ./$SERVER_JAR --spring.config.location=classpath:/application-dev2.yml &
cd -

mkdir dev3
cd dev3
mkdir classpath
cp ../../../target/$SERVER_JAR .
cp ../../../target/classes/application-dev3.yml ./classpath/
nohup java -jar ./$SERVER_JAR --spring.config.location=classpath:/application-dev3.yml &
cd -