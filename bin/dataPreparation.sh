#!/bin/bash


TPCDS_ROOT_DIR="$HOME/TPC-DS_Spark_HBase"
TPCDS_WORK_DIR=$TPCDS_ROOT_DIR"/work"
Tables="";
output_dir=$TPCDS_WORK_DIR
HOSTNAME="cloud64"

# function from tpcsspark
template(){
    # usage: template file.tpl
    while read -r line ; do
            line=${line//\"/\\\"}
            line=${line//\`/\\\`}
            line=${line//\$/\\\$}
            line=${line//\\\${/\${}
            eval "echo \"$line\"";
    done < ${1}
}

for i in `ls ${TPCDS_ROOT_DIR}/src/ddl/*.sql`
do
  baseName="$(basename $i)"
  template $i > ${output_dir}/$baseName
done 


#######################    1    ##########################
function refactorData {
	cd $TPCDS_ROOT_DIR
	
	rsync -a $TPCDS_ROOT_DIR/gendata/ $TPCDS_ROOT_DIR/validatedData/ --exclude={dbgen_version.dat,.gitkeep}
	
	delimiter="|"
	for filename in `ls ${TPCDS_ROOT_DIR}/validatedData/*.dat`
  	do
		## The command bellow adds a collumn with a counter that serves as row key
		awk -F'|' 'BEGIN{i=0}{print i"|"$0;i++}' $filename > "tmp.tmp" && mv "tmp.tmp" $filename

		file_noext=$(basename $filename .dat)

		fieldNames=$(cat ${TPCDS_ROOT_DIR}/work/create_tables.sql | pcregrep -Mo "(?<=create table "$file_noext")[\s\S]+?\)" | awk 'length($1) > 3 {print $1}')

		echo "Processing dataset: "$file_noext
   		Tables+=$file_noext" "
		
		# The script bellow creates a scala file for each table on the scalaStructs folder. Each scala file will generate JSON file with the table structure.
		python3 bin/hybrid.py $filename $delimiter "$fieldNames" $TPCDS_ROOT_DIR
	
#		mv $filename validatedData/"$(basename $filename .dat)".csv
         done
  
	( IFS=$'\n'; echo "${Tables[*]}" > tables.txt )

	# Uploading data to HDFS
  	hdfs dfs -mkdir -p $HOME/validatedData
  	hdfs dfs -copyFromLocal $TPCDS_ROOT_DIR/validatedData/* hdfs://$HOSTNAME:9000$HOME/validatedData
}


function sbtProject {
  cd $TPCDS_ROOT_DIR/scalaJobs
  mkdir -p src/{main,test}/{java,resources,scala}
  mkdir -p lib project target

  # TODO: DESMARTELAR GET CONECTOR SHC FROM LOCAL FOLDER
  cp $HOME/shc/core/target/shc-core-1.1.2-2.2-s_2.11-SNAPSHOT.jar lib/

  echo 'name := "TPCbench"
  
  version := "1.0"
  
  scalaVersion := "2.11.8"
  
  libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "2.2.0",
  "org.apache.spark" % "spark-sql_2.10" % "2.2.0"
)' > build.sbt

}

# Refactor TPCDS data and create JSON files with the data structure
function create_scala_hbaseData {
  current_dir=`pwd`
  mkdir -p $TPCDS_ROOT_DIR/scalaJobs
  mkdir -p $TPCDS_ROOT_DIR/scalaWorlandia
  mkdir -p $TPCDS_ROOT_DIR/validatedData
  sbtProject
  #refactorData
  cd $TPCDS_ROOT_DIR/scalaJobs
  echo "Compiling JAR..."
  sbt -sbt-version 0.13.8 -J-Xss512m package
 
}



########################################################### 2 #####################################
function sparkJob {
  JAR_OPTIONS=" --packages org.apache.hbase:hbase-common:1.2.6,org.apache.hbase:hbase-client:1.2.6,org.apache.hbase:hbase-protocol:1.2.6,org.apache.hbase:hbase-server:1.2.6 --jars "$HOME"/shc/core/target/shc-core-1.1.2-2.2-s_2.11-SNAPSHOT.jar"",$(echo /home/gsd/shc/target/dependency/*.jar | tr ' ' ',') --driver-class-path /home/gsd/shc/core/target/shc-core-1.1.2-2.2-s_2.11-SNAPSHOT.jar,$(echo /home/gsd/shc/target/dependency/*.jar | tr ' ' ',') --files /home/gsd/shc/core/src/main/resources/schema.xml,/home/gsd/SafeNoSQL/safeclient/src/main/resources/key.txt,hbase-site.xml --conf spark.executor.extraClassPath=/home/gsd/shc/core/target/shc-core-1.1.2-2.2-s_2.11-SNAPSHOT.jar,$(echo /home/gsd/shc/target/dependency/*.jar | tr ' ' ',')"
  CONFIG_OPTIONS=" --master local "
  spark-submit $JAR_OPTIONS $CONFIG_OPTIONS --class $1 $TPCDS_ROOT_DIR/scalaJobs/target/scala-2.11/tpcbench_2.11-1.0.jar #> /dev/null 2>&1
  cd $TPCDS_ROOT_DIR
}



function load_data_hbase {
  cd $TPCDS_ROOT_DIR

  for i in `ls ${TPCDS_ROOT_DIR}/gendata/*.dat`
    do
   	file_noext=$(basename $i .dat)
	echo "Processing job: "$file_noext "..."
      	sparkJob $file_noext
  done
}



############################################################# 3 #######################################
function sbtWork {
  cd $TPCDS_ROOT_DIR/scalaWorlandia
  mkdir -p src/{main,test}/{java,resources,scala}
  mkdir -p lib project target

  # GET CONECTOR SHC FROM LOCAL FOLDER
  cp $HOME/shc/core/target/shc-core-1.1.2-2.2-s_2.11-SNAPSHOT.jar lib/

  echo 'name := "TPCbenchWorkload"

  version := "1.0"

  scalaVersion := "2.11.8"

  libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "2.2.0",
  "org.apache.spark" % "spark-sql_2.10" % "2.2.0"
  )' > build.sbt
}

function sparkQuery {
  cd $TPCDS_ROOT_DIR
  JAR_OPTIONS=" --packages org.apache.hbase:hbase-common:1.2.6,org.apache.hbase:hbase-client:1.2.6,org.apache.hbase:hbase-protocol:1.2.6,org.apache.hbase:hbase-server:1.2.6 --jars "$HOME"/shc/core/target/shc-core-1.1.2-2.2-s_2.11-SNAPSHOT.jar"",$(echo /home/gsd/shc/target/dependency/*.jar | tr ' ' ',') --driver-class-path /home/gsd/shc/core/target/shc-core-1.1.2-2.2-s_2.11-SNAPSHOT.jar,$(echo /home/gsd/shc/target/dependency/*.jar | tr ' ' ',') --files /home/gsd/shc/core/src/main/resources/schema.xml,/home/gsd/SafeNoSQL/safeclient/src/main/resources/key.txt,hbase-site.xml --conf spark.executor.extraClassPath=/home/gsd/shc/core/target/shc-core-1.1.2-2.2-s_2.11-SNAPSHOT.jar,$(echo /home/gsd/shc/target/dependency/*.jar | tr ' ' ',')"
 
  CONFIG_OPTIONS=" --master yarn "

  spark-submit $CONFIG_OPTIONS $JAR_OPTIONS --class $1 $TPCDS_ROOT_DIR/scalaWorlandia/target/scala-2.11/tpcbenchworkload_2.11-1.0.jar > $1.output
 
  # {TPCDS_ROOT_DIR}/bin/runqueries.sh $SPARK_HOME $TPCDS_WORK_DIR  > ${TPCDS_WORK_DIR}/runqueries.out 2>&1 &

}


function runQueries {
  sbtWork
  cd $TPCDS_ROOT_DIR
  rm $TPCDS_ROOT_DIR/scalaWorlandia/src/main/scala/query*
  
  echo "Select an query interval(ex. 1~1, 10~99)"
  read interval
  array=()
  IFS='~' read -r -a array <<< $interval
  echo "Generating workload..."
  python3 $TPCDS_ROOT_DIR/bin/generateWorkload.py ${array[0]} ${array[1]} $TPCDS_ROOT_DIR
  cd $TPCDS_ROOT_DIR/scalaWorlandia
  echo "Compiling query workload..."
  sbt -sbt-version 0.13.8 package #> /dev/null 2>&1

  for filename in `ls ${TPCDS_ROOT_DIR}/scalaWorlandia/src/main/scala/query*`
  do
    name=$(basename $filename .scala)
    echo $name
    sparkQuery $name
  done
}

#######################################################################################################

function clearALL {
  cd $TPCDS_ROOT_DIR
  echo "Deleting generated data... "
  rm -rf scalaJobs scalaStructs validatedData scalaWorlandia
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
 (2) Load data into HBase 
RUN
 (3) Benchmark HBase with all (99) TPC-DS Queries
CLEANUP
 (4) Cleanup generated code and delete data from HBase
 (Q) Quit
----------------------------------------------
EOF
    printf "%s" "Please enter your choice followed by [ENTER]: "
    read option
    printf "%s\n\n" "----------------------------------------------"
    case "$option" in
    "1")  create_scala_hbaseData ;;
    "2")  load_data_hbase ;;
    "3")  runQueries ;;
    "4")  clearALL ;;
    "Q")  exit                      ;;
    "q")  exit                      ;;
     * )  echo "invalid option"     ;;
    esac
    echo "Press any key to continue"
    read -n1 -s
done
