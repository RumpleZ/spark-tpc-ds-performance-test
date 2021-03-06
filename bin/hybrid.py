import csv
import sys
from collections import defaultdict
from shutil import copy
import tempfile

all_columns = []
dict_cols = defaultdict(list)


def addCSVHeader(filename, colNames):
    result = "key"
    for col in colNames.split("\n"):
        result += "|" + col
    
    with open(filename, 'r+') as csvfile:
        content = csvfile.read()
        csvfile.seek(0)
        csvfile.write(result + "\n")
        csvfile.write(content)
#    with tempfile.NamedTemporaryFile() as temp:
 #       copy(filename, temp.name)
  #      with open(filename, 'w+') as new:
   #         with open(temp.name, 'r+') as old:
    #            new.write(result + "\n")
     #           for line in old.readlines():
      #              new.write(line)

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
            for word, i in zip(row, range(len(row))):
                dict_cols[all_columns[i]].append(word)


def imports():
    scala_SHC_catalog = "import org.apache.spark.sql.{DataFrame, SparkSession}\n"
    scala_SHC_catalog += "import java.io._\nimport org.apache.spark.sql.execution.datasources.hbase._\n"
    scala_SHC_catalog += "import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}\n\n"
    return scala_SHC_catalog


def body(DBName, delimiter, colNames):
    
    scala_SHC_catalog = "object " + DBName[DBName.rfind("/") + 1:] + "{\n  def main(args: Array[String]): Unit = {\n"

    scala_SHC_catalog += "val spark = SparkSession.builder.master(\"yarn\").appName(\"sparkHBase\").getOrCreate()\n"
    scala_SHC_catalog += "val sqlContext = spark.sqlContext\n"

    scala_SHC_catalog += "val df = spark.read.format(\"csv\").option(\"header\", \"true\").option(\"delimiter\", " + "\"" + delimiter + "\")." + \
                         "option(\"inferSchema\",\"true\").load(\"" + "hdfs://cloud52:9000/home/gsd/validatedData/" +  DBName[DBName.rfind(
        "/") + 1:] + ".csv" + "\")\n\n"

    scala_SHC_catalog += "val dfTypes = scala.collection.mutable.Map[String, String]()\n"

    scala_SHC_catalog += "for (x <- df.schema.fields) dfTypes += x.name -> x.dataType.simpleString\n\n"

    colNames = (colNames.split('\n'))

    scala_SHC_catalog += "\n\nval " + DBName[DBName.rfind("/") + 1:] + " = s\"\"\"{\n"
    scala_SHC_catalog += "                  \"table\":{\"namespace\":\"default\", \"name\":\"" + DBName[DBName.rfind(
        "/") + 1:] + "\", \"tableCoder\":\"PrimitiveType\"},\n"
    scala_SHC_catalog += "                  \"rowkey\":\"key\",\n"
    scala_SHC_catalog += "                  \"columns\":{\n"
    scala_SHC_catalog += "                  \"key\":{\"cf\":\"rowkey\", \"col\":\"key\", \"type\":\"\"\"\"" + " + dfTypes.getOrElse(\"key\", \"string\") + " + "\"\"\"\"},\n"

    for entry, i in zip(all_columns, range(len(all_columns))):
        if i == 0:  # ignore key
            continue
        scala_SHC_catalog += "                  \"" + colNames[i - 1] + "\":{\"cf\":\"" + DBName[DBName.rfind("/") + 1:]  + "\", \"col\": \"" + colNames[i - 1] + "\"" + \
                             ", \"type\":\"\"\"\" + dfTypes.getOrElse(\"" + colNames[
                                 i - 1] + "\", \"string\") + \"\"\"\"},\n"
    scala_SHC_catalog = scala_SHC_catalog[:-1][:-1] + "\n"
    scala_SHC_catalog += "                  }\n"
    scala_SHC_catalog += "                 }\"\"\".stripMargin\n\n"
    return scala_SHC_catalog


def typeMainDeriver(DBName, delimiter, colNames, rootdir):
    aux = imports()

    vBody = body(DBName, delimiter, colNames)

    aux += vBody

    scc = "val pw = new PrintWriter(new File(\"" + rootdir + "/scalaJobs/src/main/scala/" + DBName[DBName.rfind(
        "/") + 1:] + ".json" + "\" ))\n"
    scc += "pw.write(" + DBName[DBName.rfind("/") + 1:] + ")\n"
    scc += "pw.close\n"

    scc += "def withCatalog(cat: String): DataFrame = {\n"
    scc += "sqlContext.read\n"
    scc += ".options(Map(org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog.tableCatalog->cat))\n"
    scc += ".format(\"org.apache.spark.sql.execution.datasources.hbase\").load()}\n"

    scc += "df.write.options(Map(HBaseTableCatalog.tableCatalog->"+  DBName[DBName.rfind("/") + 1:] + ",HBaseTableCatalog.newTable->\"5\")).format(\"org.apache.spark.sql.execution.datasources.hbase\").save()\n"



    with open(rootdir + "/scalaJobs/src/main/scala/" + DBName[DBName.rfind("/") + 1:] + ".scala", 'w+') as file:
        file.write(aux + scc + "\nSystem.exit(0)\n}\n}")


if __name__ == '__main__':
    if len(sys.argv) != 5:
        print("Wrong arguments. python3 csv_filename.csv delimiter cols rootdir")
        exit(-1)
    addCSVHeader(sys.argv[1], sys.argv[3])
    getCSV(sys.argv[1])

    typeMainDeriver(sys.argv[1].split(".")[0], sys.argv[2], sys.argv[3], sys.argv[4])

