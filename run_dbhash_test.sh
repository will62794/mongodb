#!/bin/sh

#args
timelimit=$1
rc=$2
repeat=$3

tarball="http://downloads.mongodb.org/linux/mongodb-linux-x86_64-ubuntu1404-3.4.0-${rc}.tgz"
priv_key="/home/william/.vagrant.d/insecure_private_key"
nodes="-n 192.168.33.100 -n 192.168.33.101 -n 192.168.33.102 -n 192.168.33.103 -n 192.168.33.104"

for i in `seq 1 $repeat`;
do	
	lein run test --time-limit $timelimit --test dbhash $nodes --username vagrant --ssh-private-key $priv_key --tarball $tarball
done;

