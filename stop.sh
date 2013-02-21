program_name=`pwd`/dataload.jar
echo "$program_name"
p_id=`ps aux |grep $program_name|grep -v grep|awk -F' ' '{print $2}'`
if [ x$p_id != x'' ] 
then
        kill -9 $p_id
        echo "kill -9 $p_id"
else
        echo "program not running"
fi