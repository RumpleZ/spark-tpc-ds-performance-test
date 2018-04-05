import sys

def imports():
	scala_SHC_catalog = "import org.apache.spark.sql.{DataFrame, SparkSession}\n"
	scala_SHC_catalog += "import org.apache.spark.sql.execution.datasources.hbase\n"
	scala_SHC_catalog += "import java.io._\n"
	scala_SHC_catalog += "import org.apache.spark.sql.execution.datasources.hbase._\n\n"
	scala_SHC_catalog += "import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}\n"
	return scala_SHC_catalog


def createTables():
	aux = "\ndef withCatalog(cat: String): DataFrame = {\n"
	aux += "       sqlContext\n"
	aux += "         .read\n"
	aux += "         .options(Map(org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog.tableCatalog->cat))\n"
	aux += "         .format(\"org.apache.spark.sql.execution.datasources.hbase\")\n"
	aux += "         .load()\n"
	aux += "     }\n"
	return aux 

def main(queryNUM):
	return "object query"+queryNUM+"{\nval spark = SparkSession.builder.master(\"local\").appName(\"sparkHBase\").getOrCreate()\nval sqlContext = spark.sqlContext\n"+createTables()+"def main(args: Array[String]): Unit = {\n"



def loadCatalog(start, end, tables, rootDir):
	aux = imports()
	for x in range(int(start), int(end)+1):
		aux += main(str(x).zfill(2))
		with open("scalaWorlandia/src/main/scala/query" + str(x).zfill(2) + ".scala", "w") as wrkld:
			for table in tables:
				aux += "val source_" + table + " = scala.io.Source.fromFile(\""+rootDir+"/scalaStructs/"+table+".json\")\n"
				aux += "val "+table+" = try source_"+table+".mkString finally source_"+table+".close()\n"
				aux += "withCatalog("+table+").createOrReplaceTempView(\"" + table + "\")\n"
			aux += "val query = scala.io.Source.fromFile(\""+rootDir+"/genqueries/query"+str(x).zfill(2)+".sql\")\n"
			aux += "val querySTR = try query.mkString finally query.close()\n"
			aux += "spark.time(sqlContext.sql(querySTR.patch(querySTR.lastIndexOf(';'), \"\", 1)).collect.foreach(println))\n}\n}\n"
			wrkld.write(aux)
		aux = imports()



if __name__ == '__main__':
	if(len(sys.argv) != 4):
		print("Wrong arguments. python3 generateWorkload.py START_QUERY(1) END_QUERY(99) rootDir")
		exit(-1)
	with open("tables.txt", "r+") as tables:
		tablex = tables.readline().split(' ')[:-1]
	loadCatalog(sys.argv[1], sys.argv[2], tablex, sys.argv[3])
