import csv
from collections import defaultdict
import datetime
import sys

all_columns = []
dict_cols = defaultdict(list)

def addCSVHeader(filename, colNames):
	result = "key"
	with open(filename, 'r+') as csvfile:
		content = csvfile.read()
		csvfile.seek(0)
		for col in colNames.split("\n"):
			result += "|"+col
		csvfile.write(result+"\n")
		csvfile.write(content)


# Build a dictionary of lists to derivate the type from columns.
def getCSV(filename):
	first_line = True
	with open(filename, 'r') as csvfile:
		spamreader = csv.reader(csvfile, delimiter='|')
		for row in spamreader:
			if first_line:
				first_line = False
				for word in row:
					all_columns.append(word)
				continue
			for word, i in zip(row,range(len(row))):
				dict_cols[all_columns[i]].append(word)
					
# Given a string returns it type. Supports: int, bool, string and date. 
# There is no doubles or longs in Python3.
def typeFinder(string):
	if string == "":
		return "Ignored"
	if string == "true" or string == "false" or string == "True" or string == "False":
		return "boolean"
	if '-' in string and len(string.split('-')) == 3:
		try:
			date = string.split('-')
			if len(date[0]) == 2: # day first
				date.reverse()
			datetime.date(int(date[0]), int(date[1]), int(date[2]))
			return 'date'
		except ValueError:
			return "string"

	if '/' in string and len(string.split('/')) == 3:
		try:
			date = string.split('/')
			if len(date[0]) == 2: # day first
				date.reverse()
			datetime.date(int(date[0]), int(date[1]), int(date[2]))
			return 'date'
		except ValueError:
			return "string"

	if '.' in string:
		try:
			val = float(string)
			return "float"
		except ValueError:
			return "string"

	try:
		val = int(string)
		return "int"
	except ValueError:
		return "string"
	return "string"

def typeDeriver(stringList):
	derivedTypes = []
	__type = "string"
	for string in stringList:
		derivedTypes.append(typeFinder(string))
	
	derivedTypes = list(filter(("Ignored").__ne__, derivedTypes))

	if len(set(derivedTypes)) == 1:
		return derivedTypes[0]
	if len(set(derivedTypes)) == 2 and "Float" in derivedTypes and "Int" in derivedTypes:
		return "float"
	return "string"

def imports():
	scala_SHC_catalog = "import org.apache.spark.sql.{DataFrame, SparkSession}\n"
	scala_SHC_catalog += "import org.apache.spark.sql.execution.datasources.hbase\n"
	scala_SHC_catalog += "import java.io._\n"
	scala_SHC_catalog += "import org.apache.spark.sql.execution.datasources.hbase._\n\n"
	scala_SHC_catalog += "import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}\n\n"
	return scala_SHC_catalog

def mainDecl(DBName):
	return "object " + DBName[DBName.rfind("/")+1:] + " {\ndef main(args: Array[String]): Unit = {\n"

def body(DBName, delimiter, colNames):
	scala_SHC_catalog = "val spark = SparkSession.builder.master(\"local\").appName(\"sparkHBase\").getOrCreate()\n"
	scala_SHC_catalog += "val sqlContext = spark.sqlContext\n"

	scala_SHC_catalog += "val df = spark.read.format(\"csv\").option(\"header\", \"true\").option(\"delimiter\", " + "\"" + delimiter + "\")." +\
	"option(\"inferSchema\",\"true\").load(\"" +DBName+".csv" +"\")\n\n"

	scala_SHC_catalog += "val dfTypes = scala.collection.mutable.Map[String, String]()\n"

	scala_SHC_catalog += "for (x <- df.schema.fields) dfTypes += x.name -> x.dataType.simpleString\n\n"


	colNames = (colNames.split('\n'))

	scala_SHC_catalog += "\n\nval " + DBName[DBName.rfind("/")+1:] + " = s\"\"\"{\n"
	scala_SHC_catalog += "                  \"table\":{\"namespace\":\"default\", \"name\":\"" + DBName[DBName.rfind("/")+1:] + "\", \"tableCoder\":\"PrimitiveType\"},\n"
	scala_SHC_catalog += "                  \"rowkey\":\"key\",\n"
	scala_SHC_catalog += "                  \"columns\":{\n"
	scala_SHC_catalog += "                  \"key\":{\"cf\":\"rowkey\", \"col\":\"key\", \"type\":\"\"\"\"" + " + dfTypes.getOrElse(\"key\", \"string\") + " + "\"\"\"\"},\n"
	
	for entry, i in zip(all_columns, range(len(all_columns))):
		if i == 0: # ignore key
			continue
		scala_SHC_catalog += "                  \"" + colNames[i-1] + "\":{\"cf\":\"" + colNames[i-1] + "\", \"col\": \"" + colNames[i-1] + "\"" +\
		", \"type\":\"\"\"\" + dfTypes.getOrElse(\"" +colNames[i-1] + "\", \"string\") + \"\"\"\"},\n"
	scala_SHC_catalog = scala_SHC_catalog[:-1][:-1] + "\n"
	scala_SHC_catalog += "                  }\n"
	scala_SHC_catalog += "                 }\"\"\".stripMargin\n\n"
	return scala_SHC_catalog 
	

def typeMainDeriver(DBName, delimiter, colNames):
	
	scala_SHC_catalog = imports()
	aux = imports()

	scala_SHC_catalog += mainDecl(DBName)

	vBody = body(DBName, delimiter, colNames)
	scala_SHC_catalog += vBody
	aux += vBody

	scc = "val pw = new PrintWriter(new File(\"/home/cruz/Desktop/spark-tpc-ds-performance-test/scalaStructs/" +  DBName[DBName.rfind("/")+1:]+".json" + "\" ))\n"
	scc += "pw.write(" + DBName[DBName.rfind("/")+1:] + ")\n"
	scc += "pw.close\n"

	with open("/home/cruz/Desktop/spark-tpc-ds-performance-test/scalaStructs/" +  DBName[DBName.rfind("/")+1:]+".sc", 'w+') as file:
		file.write(aux + scc + "\nSystem.exit(0)")

	scala_SHC_catalog += "df.write.options(Map(HBaseTableCatalog.tableCatalog->" + DBName[DBName.rfind("/")+1:] + "," +\
	"HBaseTableCatalog.newTable->\"5\")).format(\"org.apache.spark.sql.execution.datasources.hbase\").save()\n}\n}"

	
	return scala_SHC_catalog



if __name__ == '__main__':
	if len(sys.argv) != 4:
		print("Wrong arguments. python3 csv_filename.csv delimiter cols")
		exit(-1)
	addCSVHeader(sys.argv[1], sys.argv[3])
	getCSV(sys.argv[1])
	scpath = sys.argv[1][:sys.argv[1][:sys.argv[1].rfind("/")].rfind("/")]+"/scalaJobs/src/main/scala/"+sys.argv[1][sys.argv[1].rfind("/"):]
	with open(scpath.split(".")[0] + ".scala", 'w+') as catalogFile:
		catalogFile.write(typeMainDeriver(sys.argv[1].split(".")[0], sys.argv[2], sys.argv[3]))
