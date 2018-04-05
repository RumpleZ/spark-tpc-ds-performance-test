import sys


def imports():
	scala_SHC_catalog = "import org.apache.spark.sql.{DataFrame, SparkSession}\n"
	scala_SHC_catalog += "import org.apache.spark.sql.execution.datasources.hbase\n"
	scala_SHC_catalog += "import java.io._\n"
	scala_SHC_catalog += "import org.apache.spark.sql.execution.datasources.hbase._\n\n"
	scala_SHC_catalog += "import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}\n"
	scala_SHC_catalog += "\nval sqlContext = spark.sqlContext\n\n"
	return scala_SHC_catalog

def createTables():
	aux = imports()
	aux += "\ndef withCatalog(cat: String): DataFrame = {\n"
	aux += "       sqlContext\n"
	aux += "         .read\n"
	aux += "         .options(Map(org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog.tableCatalog->cat))\n"
	aux += "         .format(\"org.apache.spark.sql.execution.datasources.hbase\")\n"
	aux += "         .load()\n"
	aux += "     }\n"
	return aux 

def loadCatalog(tables):
	aux	= createTables()
	with open("prepCoin.scala", 'w+') as filen:
		for table in tables:
			aux += "val source = scala.io.Source.fromFile(\"scalaStructs/"+table+".json\")\n"
			aux += "val "+table+" = try source.mkString finally source.close()\n"
			aux += "withCatalog("+table+").createOrReplaceTempView(\"" + table + "\")\n"
		filen.write(aux)

if __name__ == '__main__':
	if len(sys.argv) != 2:
		print("Wrong arguments. python3 tablenames")
		exit(-1)
	createTables()
	loadCatalog(sys.argv[1].split(' ')[:-1])