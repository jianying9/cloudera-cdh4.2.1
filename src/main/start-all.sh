#!/bin/sh
sudo service krb5-admin-server restart
sudo service krb5-kdc restart
sudo service hadoop-hdfs-namenode start
sudo service hadoop-hdfs-datanode start
sudo service hadoop-0.20-mapreduce-jobtracker start
sudo service hadoop-0.20-mapreduce-tasktracker start
sudo service zookeeper-server start
sudo service hbase-master start
sudo service hbase-regionserver start






