#! /bin/sh
while [ 1 ] 
do  
	sleep 5
	pid=`/sbin/pidof relay_fetch`
	if [ "A$pid" == "A" ];then
			sudo ./relay_fetch -uroot -D
	fi  
done
