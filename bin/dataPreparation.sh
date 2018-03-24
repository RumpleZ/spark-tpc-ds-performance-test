#!/bin/bash


TPCDS_ROOT_DIR="/home/cruz/Desktop/spark-tpc-ds-performance-test"
Tables="";


#####################################################  1  #############################################
function createAux {
  cd $TPCDS_ROOT_DIR
	# Creates scala code and parses csv data
	delimiter="|"
	for filename in `ls ${TPCDS_ROOT_DIR}/validatedData/*.dat`
  	do
		awk -F'|' 'BEGIN{i=0}{print i"|"$0;i++}' $filename > "tmp.tmp" && mv "tmp.tmp" $filename

		file_noext=$(echo $filename|cut -d'.' -f1| cut -d'/' -f7) 

		fieldNames=$(cat ${TPCDS_ROOT_DIR}/work/create_tables.sql | pcregrep -Mo "(?<=create table "$file_noext"_text)[\s\S]+?\)" | awk 'length($1) > 3 {print $1}')

		echo "Processing dataset: "$file_noext
    Tables+=$file_noext" "
		
		python3 typeFinder.py $filename $delimiter "$fieldNames"

		if [ $(echo $filename |cut -d'.' -f2) != "csv" ]
		then 
			mv $filename $(echo $filename |cut -d'.' -f1).csv
		fi
  done
  ( IFS=$'\n'; echo "${Tables[*]}" > tables.txt )
}


function sbtProject {
  cd $TPCDS_ROOT_DIR/scalaJobs
  mkdir -p src/{main,test}/{java,resources,scala}
  mkdir -p lib project target

  # GET CONECTOR SHC FROM LOCAL FOLDER
  cp /home/cruz/Desktop/shc/core/target/shc-core-1.1.2-2.2-s_2.11-SNAPSHOT.jar lib/

  echo 'name := "TPCbench"
  version := "1.0"
  scalaVersion := "2.11.8"

  libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.0"
  libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.0"' > build.sbt
}

function create_scala_hbaseData {
  current_dir=`pwd`
  mkdir -p scalaJobs
  mkdir -p scalaStructs
  mkdir -p scalaWorlandia
  mkdir -p validatedData
  sbtProject
  for i in `ls ${TPCDS_ROOT_DIR}/gendata/*.dat`
  do
    cp $i $TPCDS_ROOT_DIR"/validatedData/"
  done
  rm $TPCDS_ROOT_DIR"/validatedData/dbgen_version.dat"
  createAux
  cd $TPCDS_ROOT_DIR/scalaJobs
  echo "Compiling JAR..."
 # sbt package > /dev/null 2>&1
  cd $TPCDS_ROOT_DIR
  #python3 prepareTables.py "$Tables"

}

#######################################################################################################



########################################################### 2 ###############################
function create_scala_structs {
  for i in `ls ${TPCDS_ROOT_DIR}/scalaStructs/*.sc`
  do
    echo "Processing "$1"..."
    sudo /usr/local/spark/bin/spark-shell --master local --jars ~/Desktop/shc/core/target/shc-core-1.1.2-2.2-s_2.11-SNAPSHOT.jar -i $i > /dev/null 2>&1
  done
 
}

############################################################################################

########################################################### 3 #####################################
function sparkJob {
	cd $SPARK_HOME
  JAR_OPTIONS=" --jars /home/cruz/Desktop/shc/core/target/shc-core-1.1.2-2.2-s_2.11-SNAPSHOT.jar"
  CONFIG_OPTIONS=" --master local "
  sudo bin/spark-submit $JAR_OPTIONS $CONFIG_OPTIONS --class $1 $TPCDS_ROOT_DIR/scalaJobs/target/scala-2.11/tpcbench_2.11-1.0.jar #> /dev/null 2>&1
  cd $TPCDS_ROOT_DIR
}



function load_data_hbase {
  for i in `ls ${TPCDS_ROOT_DIR}/gendata/*.dat`
    do
      file_noext=$(echo $i|cut -d'.' -f1| cut -d'/' -f7) 
      echo "Processing job: "$file_noext "..."
      sparkJob $file_noext
  done
}
#######################################################################################################

############################################################# 4 #######################################
function sbtWork {
  cd $TPCDS_ROOT_DIR/scalaWorlandia
  mkdir -p src/{main,test}/{java,resources,scala}
  mkdir -p lib project target

  # GET CONECTOR SHC FROM LOCAL FOLDER
  cp /home/cruz/Desktop/shc/core/target/shc-core-1.1.2-2.2-s_2.11-SNAPSHOT.jar lib/

  echo 'name := "TPCbenchWorkload"
  version := "1.0"
  scalaVersion := "2.11.8"

  libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.0"
  libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.0"' > build.sbt
}

function sparkQuery {
  cd $SPARK_HOME
  JAR_OPTIONS=" --jars /home/cruz/Desktop/shc/core/target/shc-core-1.1.2-2.2-s_2.11-SNAPSHOT.jar"
  CONFIG_OPTIONS=" --master local "
  DRIVER_OPTIONS=" --driver-memory 4g --driver-java-options -Dlog4j.configuration=file:///${TPCDS_ROOT_DIR}/work/log4j.properties"
  sudo bin/spark-submit --class $1 $TPCDS_ROOT_DIR/scalaWorlandia/target/scala-2.11/tpcbenchworkload_2.11-1.0.jar $DRIVER_OPTIONS $JAR_OPTIONS $CONFIG_OPTIONS
 
  # {TPCDS_ROOT_DIR}/bin/runqueries.sh $SPARK_HOME $TPCDS_WORK_DIR  > ${TPCDS_WORK_DIR}/runqueries.out 2>&1 &

}


function runQueries {
  sbtWork
  cd $TPCDS_ROOT_DIR
  #rm $TPCDS_ROOT_DIR/scalaWorlandia/src/main/scala/query*
  
  echo "Select an query interval(ex. 1~1, 10~99)"
  read interval
  array=()
  IFS='~' read -r -a array <<< $interval
  #python3 generateWorkload.py ${array[0]} ${array[1]}
  cd $TPCDS_ROOT_DIR/scalaWorlandia
  #sbt package

  for filename in `ls ${TPCDS_ROOT_DIR}/scalaWorlandia/src/main/scala/query*`
  do
    name=$(echo $filename|cut -d'.' -f1| cut -d'/' -f10)
    echo $name
    sparkQuery $1
    exit
  done
}

#######################################################################################################

function clearALL {
  cd $TPCDS_ROOT_DIR
  echo "Deleting generated data... "
  rm -rf scalaJobs scalaStructs validatedData
  echo "Disabling table call_center..."
  echo -e "disable 'call_center'" | hbase -n  > /dev/null 2>&1
  echo "Disabling table catalog_page..."
  echo -e "disable 'catalog_page'" | hbase -n  > /dev/null 2>&1
  echo "Disabling table catalog_returns..."
  echo -e "disable 'catalog_returns'" | hbase -n  > /dev/null 2>&1
  echo "Disabling table catalog_sales..."
  echo -e "disable 'catalog_sales'" | hbase -n  > /dev/null 2>&1
  echo "Disabling table customer..."
  echo -e "disable 'customer'" | hbase -n  > /dev/null 2>&1
  echo "Disabling table customer_address..."
  echo -e "disable 'customer_address'" | hbase -n  > /dev/null 2>&1
  echo "Disabling table customer_demographics..."
  echo -e "disable 'customer_demographics'" | hbase -n  > /dev/null 2>&1
  echo "Disabling table date_dim..."
  echo -e "disable 'date_dim'" | hbase -n  > /dev/null 2>&1
  echo "Disabling table household_demographics..."
  echo -e "disable 'household_demographics'" | hbase -n  > /dev/null 2>&1
  echo "Disabling table income_band..."
  echo -e "disable 'income_band'" | hbase -n  > /dev/null 2>&1
  echo "Disabling table inventory..."
  echo -e "disable 'inventory'" | hbase -n  > /dev/null 2>&1
  echo "Disabling table item..."
  echo -e "disable 'item'" | hbase -n  > /dev/null 2>&1
  echo "Disabling table promotion..."
  echo -e "disable 'promotion'" | hbase -n  > /dev/null 2>&1
  echo "Disabling table reason..."
  echo -e "disable 'reason'" | hbase -n  > /dev/null 2>&1
  echo "Disabling table ship_mode..."
  echo -e "disable 'ship_mode'" | hbase -n  > /dev/null 2>&1
  echo "Disabling table store..."
  echo -e "disable 'store'" | hbase -n  > /dev/null 2>&1
  echo "Disabling table store_returns..."
  echo -e "disable 'store_returns'"|hbase -n  > /dev/null 2>&1
  echo "Disabling table store_sales..."
  echo -e "disable 'store_sales'"|hbase -n  > /dev/null 2>&1
  echo "Disabling table time_dim..."
  echo -e "disable 'time_dim'" | hbase -n  > /dev/null 2>&1
  echo "Disabling table warehouse..."
  echo -e "disable 'warehouse'"|hbase -n  > /dev/null 2>&1
  echo "Disabling table web_page..." 
  echo -e "disable 'web_page'"| hbase -n  > /dev/null 2>&1
  echo "Disabling table web_returns..." 
  echo -e "disable 'web_returns'"|hbase -n  > /dev/null 2>&1
  echo "Disabling table web_sales..."
  echo -e "disable 'web_sales'"|hbase -n  > /dev/null 2>&1
  echo "Disabling table web_site..."
  echo -e "disable 'web_site'"| hbase -n  > /dev/null 2>&1

  echo "Dropping table call_center..."
  echo -e "drop 'call_center'" | hbase -n  > /dev/null 2>&1
  echo "Dropping table catalog_page..."
  echo -e "drop 'catalog_page'" | hbase -n  > /dev/null 2>&1
  echo "Dropping table catalog_returns..."
  echo -e "drop 'catalog_returns'" | hbase -n  > /dev/null 2>&1
  echo "Dropping table catalog_sales..."
  echo -e "drop 'catalog_sales'" | hbase -n  > /dev/null 2>&1
  echo "Dropping table customer..."
  echo -e "drop 'customer'" | hbase -n  > /dev/null 2>&1
  echo "Dropping table customer_address..."
  echo -e "drop 'customer_address'" | hbase -n  > /dev/null 2>&1
  echo "Dropping table customer_demographics..."
  echo -e "drop 'customer_demographics'" | hbase -n  > /dev/null 2>&1
  echo "Dropping table date_dim..."
  echo -e "drop 'date_dim'" | hbase -n  > /dev/null 2>&1
  echo "Dropping table household_demographics..."
  echo -e "drop 'household_demographics'" | hbase -n  > /dev/null 2>&1
  echo "Dropping table income_band..."
  echo -e "drop 'income_band'" | hbase -n  > /dev/null 2>&1
  echo "Dropping table inventory..."
  echo -e "drop 'inventory'" | hbase -n  > /dev/null 2>&1
  echo "Dropping table item..."
  echo -e "drop 'item'" | hbase -n  > /dev/null 2>&1
  echo "Dropping table promotion..."
  echo -e "drop 'promotion'" | hbase -n  > /dev/null 2>&1
  echo "Dropping table reason..."
  echo -e "drop 'reason'" | hbase -n  > /dev/null 2>&1
  echo "Dropping table ship_mode..."
  echo -e "drop 'ship_mode'" | hbase -n  > /dev/null 2>&1
  echo "Dropping table store..."
  echo -e "drop 'store'" | hbase -n  > /dev/null 2>&1
  echo "Dropping table store_returns..."
  echo -e "drop 'store_returns'"|hbase -n  > /dev/null 2>&1
  echo "Dropping table store_sales..."
  echo -e "drop 'store_sales'"|hbase -n  > /dev/null 2>&1
  echo "Dropping table time_dim..."
  echo -e "drop 'time_dim'" | hbase -n  > /dev/null 2>&1
  echo "Dropping table warehouse..."
  echo -e "drop 'warehouse'"|hbase -n  > /dev/null 2>&1
  echo "Dropping table web_page..." 
  echo -e "drop 'web_page'"| hbase -n  > /dev/null 2>&1
  echo "Dropping table web_returns..." 
  echo -e "drop 'web_returns'"|hbase -n  > /dev/null 2>&1
  echo "Dropping table web_sales..."
  echo -e "drop 'web_sales'"|hbase -n  > /dev/null 2>&1
  echo "Dropping table web_site..."
  echo -e "drop 'web_site'"| hbase -n  > /dev/null 2>&1
}

while :
do
    clear
    cat<<EOF
==============================================
TPC-DS On Spark Menu
----------------------------------------------
SETUP
 (1) Generate scala code and parse data
 (2) Generate data structures
 (3) Load data in HBase 
RUN
 (4) Benchmark HBase with all (99) TPC-DS Queries
CLEANUP
 (5) Cleanup generated code and delete data from HBase
 (Q) Quit
----------------------------------------------
EOF
    printf "%s" "Please enter your choice followed by [ENTER]: "
    read option
    printf "%s\n\n" "----------------------------------------------"
    case "$option" in
    "1")  create_scala_hbaseData ;;
    "2")  create_scala_structs ;;
    "3")  load_data_hbase ;;
    "4")  runQueries ;;
    "5")  clearALL ;;
    "Q")  exit                      ;;
    "q")  exit                      ;;
     * )  echo "invalid option"     ;;
    esac
    echo "Press any key to continue"
    read -n1 -s
done
