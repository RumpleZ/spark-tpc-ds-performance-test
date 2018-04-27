#!/bin/bash 

SparkConf=~/TPC-DS_Spark_HBase/conf

mkdir -p $SparkConf
touch $SparkConf/spark.conf
while :
do
    clear
echo '###|  Configuration  ####'
cat $SparkConf/spark.conf
echo '####  Configuration  |###'
echo
    cat<<EOF
echo "Menu"
echo "(1)Add configuration."
echo "(2)Alter configuration."
echo "(3)Delete configuration."
echo "(0)Exit"
EOF
read option
case "$option" in
"0") exit ;;
"1") echo "Key to add:"
read newKey
echo "Value:"
read newValue
echo $newKey' '$newValue >> $SparkConf/spark.conf ;;
"2") echo "Key to alter:"
read altKey
echo "New value:"
read newValue
sed -i 's/^'$altKey'.*/'$altKey' '$newValue'/' $SparkConf/spark.conf ;;
"3") echo "Key to delete:"
read byeKey
sed -i '/^'$byeKey'/ d' $SparkConf/spark.conf ;;
* )  echo "invalid option" ;;
esac

done
