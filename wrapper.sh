#!/bin/sh
SERVICE_NAME=LagMonitor
CURDIR=`pwd`
PATH_TO_JAR="$CURDIR/LagMonitor-all.jar"
PID_PATH_NAME="$CURDIR/service.pid"
case $1 in
    start)
        echo "Starting $SERVICE_NAME ..."
        if [ ! -f $PID_PATH_NAME ]; then
            nohup java -Dlogback.configurationFile=./logback.xml -jar $PATH_TO_JAR /tmp 2>> /dev/null >> /dev/null &
		#nohup java -jar $PATH_TO_JAR >> log.file 2>&1 &
                        echo $! > $PID_PATH_NAME
            echo "$SERVICE_NAME started ..."
        else
            echo "$SERVICE_NAME is already running ..."
        fi
    ;;
    stop)
        if [ -f $PID_PATH_NAME ]; then
            PID=$(cat $PID_PATH_NAME);
            echo "$SERVICE_NAME stopping ..."
            kill $PID;
            echo "$SERVICE_NAME stopped ..."
            rm $PID_PATH_NAME
        else
            echo "$SERVICE_NAME is not running ..."
        fi
    ;;
    restart)
	$0 stop
	echo "Pause for 3 seconds"
	sleep 3
	$0 start
    ;;
esac 
