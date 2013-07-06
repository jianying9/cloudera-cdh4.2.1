#!/bin/sh
sudo service hbase-regionserver stop
sudo service hbase-master stop
sudo service zookeeper-server stop
sudo service hadoop-0.20-mapreduce-tasktracker stop
sudo service hadoop-0.20-mapreduce-jobtracker stop
sudo service hadoop-hdfs-datanode stop
sudo service hadoop-hdfs-secondarynamenode stop
sudo service hadoop-hdfs-namenode stop
sudo service krb5-admin-server stop
sudo service krb5-kdc stop
