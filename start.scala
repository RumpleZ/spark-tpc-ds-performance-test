import org.apache.spark.sql.{DataFrame, SparkSession}
import java.io._
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}
import org.apache.spark.sql.execution.datasources.hbase
import org.apache.spark.sql.execution.datasources.hbase._

val spark = SparkSession.builder.master("yarn").appName("sparkHBase").getOrCreate()
val sqlContext = spark.sqlContext

def withCatalog(cat: String): DataFrame = {
  sqlContext
  .read
  .options(Map(HBaseTableCatalog.tableCatalog->cat))
  .format("org.apache.spark.sql.execution.datasources.hbase")
  .load()
}

val call_center = s"""{
                  "table":{"namespace":"default", "name":"call_center", "tableCoder":"PrimitiveType"},
                  "rowkey":"key",
                  "columns":{
                  "key":{"cf":"rowkey", "col":"key", "type":"int"},
                  "cc_call_center_sk":{"cf":"cc_call_center_sk", "col": "cc_call_center_sk", "type":"int"},
                  "cc_call_center_id":{"cf":"cc_call_center_id", "col": "cc_call_center_id", "type":"string"},
                  "cc_rec_start_date":{"cf":"cc_rec_start_date", "col": "cc_rec_start_date", "type":"timestamp"},
                  "cc_rec_end_date":{"cf":"cc_rec_end_date", "col": "cc_rec_end_date", "type":"timestamp"},
                  "cc_closed_date_sk":{"cf":"cc_closed_date_sk", "col": "cc_closed_date_sk", "type":"string"},
                  "cc_open_date_sk":{"cf":"cc_open_date_sk", "col": "cc_open_date_sk", "type":"int"},
                  "cc_name":{"cf":"cc_name", "col": "cc_name", "type":"string"},
                  "cc_class":{"cf":"cc_class", "col": "cc_class", "type":"string"},
                  "cc_employees":{"cf":"cc_employees", "col": "cc_employees", "type":"int"},
                  "cc_sq_ft":{"cf":"cc_sq_ft", "col": "cc_sq_ft", "type":"int"},
                  "cc_hours":{"cf":"cc_hours", "col": "cc_hours", "type":"string"},
                  "cc_manager":{"cf":"cc_manager", "col": "cc_manager", "type":"string"},
                  "cc_mkt_id":{"cf":"cc_mkt_id", "col": "cc_mkt_id", "type":"int"},
                  "cc_mkt_class":{"cf":"cc_mkt_class", "col": "cc_mkt_class", "type":"string"},
                  "cc_mkt_desc":{"cf":"cc_mkt_desc", "col": "cc_mkt_desc", "type":"string"},
                  "cc_market_manager":{"cf":"cc_market_manager", "col": "cc_market_manager", "type":"string"},
                  "cc_division":{"cf":"cc_division", "col": "cc_division", "type":"int"},
                  "cc_division_name":{"cf":"cc_division_name", "col": "cc_division_name", "type":"string"},
                  "cc_company":{"cf":"cc_company", "col": "cc_company", "type":"int"},
                  "cc_company_name":{"cf":"cc_company_name", "col": "cc_company_name", "type":"string"},
                  "cc_street_number":{"cf":"cc_street_number", "col": "cc_street_number", "type":"int"},
                  "cc_street_name":{"cf":"cc_street_name", "col": "cc_street_name", "type":"string"},
                  "cc_street_type":{"cf":"cc_street_type", "col": "cc_street_type", "type":"string"},
                  "cc_suite_number":{"cf":"cc_suite_number", "col": "cc_suite_number", "type":"string"},
                  "cc_city":{"cf":"cc_city", "col": "cc_city", "type":"string"},
                  "cc_county":{"cf":"cc_county", "col": "cc_county", "type":"string"},
                  "cc_state":{"cf":"cc_state", "col": "cc_state", "type":"string"},
                  "cc_zip":{"cf":"cc_zip", "col": "cc_zip", "type":"int"},
                  "cc_country":{"cf":"cc_country", "col": "cc_country", "type":"string"},
                  "cc_gmt_offset":{"cf":"cc_gmt_offset", "col": "cc_gmt_offset", "type":"int"},
                  "cc_tax_percentage":{"cf":"cc_tax_percentage", "col": "cc_tax_percentage", "type":"double"}
                  }
                 }""".stripMargin


val customer = s"""{
                  "table":{"namespace":"default", "name":"customer", "tableCoder":"PrimitiveType"},
                  "rowkey":"key",
                  "columns":{
                  "key":{"cf":"rowkey", "col":"key", "type":"int"},
                  "c_customer_sk":{"cf":"c_customer_sk", "col": "c_customer_sk", "type":"int"},
                  "c_customer_id":{"cf":"c_customer_id", "col": "c_customer_id", "type":"string"},
                  "c_current_cdemo_sk":{"cf":"c_current_cdemo_sk", "col": "c_current_cdemo_sk", "type":"int"},
                  "c_current_hdemo_sk":{"cf":"c_current_hdemo_sk", "col": "c_current_hdemo_sk", "type":"int"},
                  "c_current_addr_sk":{"cf":"c_current_addr_sk", "col": "c_current_addr_sk", "type":"int"},
                  "c_first_shipto_date_sk":{"cf":"c_first_shipto_date_sk", "col": "c_first_shipto_date_sk", "type":"int"},
                  "c_first_sales_date_sk":{"cf":"c_first_sales_date_sk", "col": "c_first_sales_date_sk", "type":"int"},
                  "c_salutation":{"cf":"c_salutation", "col": "c_salutation", "type":"string"},
                  "c_first_name":{"cf":"c_first_name", "col": "c_first_name", "type":"string"},
                  "c_last_name":{"cf":"c_last_name", "col": "c_last_name", "type":"string"},
                  "c_preferred_cust_flag":{"cf":"c_preferred_cust_flag", "col": "c_preferred_cust_flag", "type":"string"},
                  "c_birth_day":{"cf":"c_birth_day", "col": "c_birth_day", "type":"int"},
                  "c_birth_month":{"cf":"c_birth_month", "col": "c_birth_month", "type":"int"},
                  "c_birth_year":{"cf":"c_birth_year", "col": "c_birth_year", "type":"int"},
                  "c_birth_country":{"cf":"c_birth_country", "col": "c_birth_country", "type":"string"},
                  "c_login":{"cf":"c_login", "col": "c_login", "type":"string"},
                  "c_email_address":{"cf":"c_email_address", "col": "c_email_address", "type":"string"},
                  "c_last_review_date":{"cf":"c_last_review_date", "col": "c_last_review_date", "type":"int"}
                  }
                 }"""

val store = s"""{
                  "table":{"namespace":"default", "name":"store", "tableCoder":"PrimitiveType"},
                  "rowkey":"key",
                  "columns":{
                  "key":{"cf":"rowkey", "col":"key", "type":"int"},
                  "s_store_sk":{"cf":"s_store_sk", "col": "s_store_sk", "type":"int"},
                  "s_store_id":{"cf":"s_store_id", "col": "s_store_id", "type":"string"},
                  "s_rec_start_date":{"cf":"s_rec_start_date", "col": "s_rec_start_date", "type":"timestamp"},
                  "s_rec_end_date":{"cf":"s_rec_end_date", "col": "s_rec_end_date", "type":"timestamp"},
                  "s_closed_date_sk":{"cf":"s_closed_date_sk", "col": "s_closed_date_sk", "type":"int"},
                  "s_store_name":{"cf":"s_store_name", "col": "s_store_name", "type":"string"},
                  "s_number_employees":{"cf":"s_number_employees", "col": "s_number_employees", "type":"int"},
                  "s_floor_space":{"cf":"s_floor_space", "col": "s_floor_space", "type":"int"},
                  "s_hours":{"cf":"s_hours", "col": "s_hours", "type":"string"},
                  "s_manager":{"cf":"s_manager", "col": "s_manager", "type":"string"},
                  "s_market_id":{"cf":"s_market_id", "col": "s_market_id", "type":"int"},
                  "s_geography_class":{"cf":"s_geography_class", "col": "s_geography_class", "type":"string"},
                  "s_market_desc":{"cf":"s_market_desc", "col": "s_market_desc", "type":"string"},
                  "s_market_manager":{"cf":"s_market_manager", "col": "s_market_manager", "type":"string"},
                  "s_division_id":{"cf":"s_division_id", "col": "s_division_id", "type":"int"},
                  "s_division_name":{"cf":"s_division_name", "col": "s_division_name", "type":"string"},
                  "s_company_id":{"cf":"s_company_id", "col": "s_company_id", "type":"int"},
                  "s_company_name":{"cf":"s_company_name", "col": "s_company_name", "type":"string"},
                  "s_street_number":{"cf":"s_street_number", "col": "s_street_number", "type":"int"},
                  "s_street_name":{"cf":"s_street_name", "col": "s_street_name", "type":"string"},
                  "s_street_type":{"cf":"s_street_type", "col": "s_street_type", "type":"string"},
                  "s_suite_number":{"cf":"s_suite_number", "col": "s_suite_number", "type":"string"},
                  "s_city":{"cf":"s_city", "col": "s_city", "type":"string"},
                  "s_county":{"cf":"s_county", "col": "s_county", "type":"string"},
                  "s_state":{"cf":"s_state", "col": "s_state", "type":"string"},
                  "s_zip":{"cf":"s_zip", "col": "s_zip", "type":"int"},
                  "s_country":{"cf":"s_country", "col": "s_country", "type":"string"},
                  "s_gmt_offset":{"cf":"s_gmt_offset", "col": "s_gmt_offset", "type":"int"},
                  "s_tax_precentage":{"cf":"s_tax_precentage", "col": "s_tax_precentage", "type":"double"}
                  }
                 }"""

val store_returns = s"""{
                  "table":{"namespace":"default", "name":"store_returns", "tableCoder":"PrimitiveType"},
                  "rowkey":"key",
                  "columns":{
                  "key":{"cf":"rowkey", "col":"key", "type":"int"},
                  "sr_returned_date_sk":{"cf":"sr_returned_date_sk", "col": "sr_returned_date_sk", "type":"int"},
                  "sr_return_time_sk":{"cf":"sr_return_time_sk", "col": "sr_return_time_sk", "type":"int"},
                  "sr_item_sk":{"cf":"sr_item_sk", "col": "sr_item_sk", "type":"int"},
                  "sr_customer_sk":{"cf":"sr_customer_sk", "col": "sr_customer_sk", "type":"int"},
                  "sr_cdemo_sk":{"cf":"sr_cdemo_sk", "col": "sr_cdemo_sk", "type":"int"},
                  "sr_hdemo_sk":{"cf":"sr_hdemo_sk", "col": "sr_hdemo_sk", "type":"int"},
                  "sr_addr_sk":{"cf":"sr_addr_sk", "col": "sr_addr_sk", "type":"int"},
                  "sr_store_sk":{"cf":"sr_store_sk", "col": "sr_store_sk", "type":"int"},
                  "sr_reason_sk":{"cf":"sr_reason_sk", "col": "sr_reason_sk", "type":"int"},
                  "sr_ticket_number":{"cf":"sr_ticket_number", "col": "sr_ticket_number", "type":"int"},
                  "sr_return_quantity":{"cf":"sr_return_quantity", "col": "sr_return_quantity", "type":"int"},
                  "sr_return_amt":{"cf":"sr_return_amt", "col": "sr_return_amt", "type":"double"},
                  "sr_return_tax":{"cf":"sr_return_tax", "col": "sr_return_tax", "type":"double"},
                  "sr_return_amt_inc_tax":{"cf":"sr_return_amt_inc_tax", "col": "sr_return_amt_inc_tax", "type":"double"},
                  "sr_fee":{"cf":"sr_fee", "col": "sr_fee", "type":"double"},
                  "sr_return_ship_cost":{"cf":"sr_return_ship_cost", "col": "sr_return_ship_cost", "type":"double"},
                  "sr_refunded_cash":{"cf":"sr_refunded_cash", "col": "sr_refunded_cash", "type":"double"},
                  "sr_reversed_charge":{"cf":"sr_reversed_charge", "col": "sr_reversed_charge", "type":"double"},
                  "sr_store_credit":{"cf":"sr_store_credit", "col": "sr_store_credit", "type":"double"},
                  "sr_net_loss":{"cf":"sr_net_loss", "col": "sr_net_loss", "type":"double"}
                  }
                 }"""

val date_dim = s"""{
                  "table":{"namespace":"default", "name":"date_dim", "tableCoder":"PrimitiveType"},
                  "rowkey":"key",
                  "columns":{
                  "key":{"cf":"rowkey", "col":"key", "type":"int"},
                  "d_date_sk":{"cf":"d_date_sk", "col": "d_date_sk", "type":"int"},
                  "d_date_id":{"cf":"d_date_id", "col": "d_date_id", "type":"string"},
                  "d_date":{"cf":"d_date", "col": "d_date", "type":"timestamp"},
                  "d_month_seq":{"cf":"d_month_seq", "col": "d_month_seq", "type":"int"},
                  "d_week_seq":{"cf":"d_week_seq", "col": "d_week_seq", "type":"int"},
                  "d_quarter_seq":{"cf":"d_quarter_seq", "col": "d_quarter_seq", "type":"int"},
                  "d_year":{"cf":"d_year", "col": "d_year", "type":"int"},
                  "d_dow":{"cf":"d_dow", "col": "d_dow", "type":"int"},
                  "d_moy":{"cf":"d_moy", "col": "d_moy", "type":"int"},
                  "d_dom":{"cf":"d_dom", "col": "d_dom", "type":"int"},
                  "d_qoy":{"cf":"d_qoy", "col": "d_qoy", "type":"int"},
                  "d_fy_year":{"cf":"d_fy_year", "col": "d_fy_year", "type":"int"},
                  "d_fy_quarter_seq":{"cf":"d_fy_quarter_seq", "col": "d_fy_quarter_seq", "type":"int"},
                  "d_fy_week_seq":{"cf":"d_fy_week_seq", "col": "d_fy_week_seq", "type":"int"},
                  "d_day_name":{"cf":"d_day_name", "col": "d_day_name", "type":"string"},
                  "d_quarter_name":{"cf":"d_quarter_name", "col": "d_quarter_name", "type":"string"},
                  "d_holiday":{"cf":"d_holiday", "col": "d_holiday", "type":"string"},
                  "d_weekend":{"cf":"d_weekend", "col": "d_weekend", "type":"string"},
                  "d_following_holiday":{"cf":"d_following_holiday", "col": "d_following_holiday", "type":"string"},
                  "d_first_dom":{"cf":"d_first_dom", "col": "d_first_dom", "type":"int"},
                  "d_last_dom":{"cf":"d_last_dom", "col": "d_last_dom", "type":"int"},
                  "d_same_day_ly":{"cf":"d_same_day_ly", "col": "d_same_day_ly", "type":"int"},
                  "d_same_day_lq":{"cf":"d_same_day_lq", "col": "d_same_day_lq", "type":"int"},
                  "d_current_day":{"cf":"d_current_day", "col": "d_current_day", "type":"string"},
                  "d_current_week":{"cf":"d_current_week", "col": "d_current_week", "type":"string"},
                  "d_current_month":{"cf":"d_current_month", "col": "d_current_month", "type":"string"},
                  "d_current_quarter":{"cf":"d_current_quarter", "col": "d_current_quarter", "type":"string"},
                  "d_current_year":{"cf":"d_current_year", "col": "d_current_year", "type":"string"}
                  }
                 }"""

withCatalog(customer).createOrReplaceTempView("customer")
withCatalog(date_dim).createOrReplaceTempView("date_dim")
withCatalog(store).createOrReplaceTempView("store")
withCatalog(store_returns).createOrReplaceTempView("store_returns")

//sql("with customer_total_return as (select sr_customer_sk as ctr_customer_sk ,sr_store_sk as ctr_store_sk ,sum(SR_RETURN_AMT) as ctr_total_return from store_returns ,date_dim where sr_returned_date_sk = d_date_sk and d_year =2000 group by sr_customer_sk ,sr_store_sk)  select  c_customer_id from customer_total_return ctr1 ,store ,customer where ctr1.ctr_total_return > (select avg(ctr_total_return)*1.2 from customer_total_return ctr2 where ctr1.ctr_store_sk = ctr2.ctr_store_sk) and s_store_sk = ctr1.ctr_store_sk and s_state = 'TN' and ctr1.ctr_customer_sk = c_customer_sk order by c_customer_id  limit 100").show
import org.apache.spark.sql.{DataFrame, SparkSession}
import java.io._
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}
import org.apache.spark.sql.execution.datasources.hbase
import org.apache.spark.sql.execution.datasources.hbase._

val spark = SparkSession.builder.master("yarn").appName("sparkHBase").getOrCreate()
val sqlContext = spark.sqlContext

def withCatalog(cat: String): DataFrame = {
  sqlContext
  .read
  .options(Map(HBaseTableCatalog.tableCatalog->cat))
  .format("org.apache.spark.sql.execution.datasources.hbase")
  .load()
}

val call_center = s"""{
                  "table":{"namespace":"default", "name":"call_center", "tableCoder":"PrimitiveType"},
                  "rowkey":"key",
                  "columns":{
                  "key":{"cf":"rowkey", "col":"key", "type":"int"},
                  "cc_call_center_sk":{"cf":"cc_call_center_sk", "col": "cc_call_center_sk", "type":"int"},
                  "cc_call_center_id":{"cf":"cc_call_center_id", "col": "cc_call_center_id", "type":"string"},
                  "cc_rec_start_date":{"cf":"cc_rec_start_date", "col": "cc_rec_start_date", "type":"timestamp"},
                  "cc_rec_end_date":{"cf":"cc_rec_end_date", "col": "cc_rec_end_date", "type":"timestamp"},
                  "cc_closed_date_sk":{"cf":"cc_closed_date_sk", "col": "cc_closed_date_sk", "type":"string"},
                  "cc_open_date_sk":{"cf":"cc_open_date_sk", "col": "cc_open_date_sk", "type":"int"},
                  "cc_name":{"cf":"cc_name", "col": "cc_name", "type":"string"},
                  "cc_class":{"cf":"cc_class", "col": "cc_class", "type":"string"},
                  "cc_employees":{"cf":"cc_employees", "col": "cc_employees", "type":"int"},
                  "cc_sq_ft":{"cf":"cc_sq_ft", "col": "cc_sq_ft", "type":"int"},
                  "cc_hours":{"cf":"cc_hours", "col": "cc_hours", "type":"string"},
                  "cc_manager":{"cf":"cc_manager", "col": "cc_manager", "type":"string"},
                  "cc_mkt_id":{"cf":"cc_mkt_id", "col": "cc_mkt_id", "type":"int"},
                  "cc_mkt_class":{"cf":"cc_mkt_class", "col": "cc_mkt_class", "type":"string"},
                  "cc_mkt_desc":{"cf":"cc_mkt_desc", "col": "cc_mkt_desc", "type":"string"},
                  "cc_market_manager":{"cf":"cc_market_manager", "col": "cc_market_manager", "type":"string"},
                  "cc_division":{"cf":"cc_division", "col": "cc_division", "type":"int"},
                  "cc_division_name":{"cf":"cc_division_name", "col": "cc_division_name", "type":"string"},
                  "cc_company":{"cf":"cc_company", "col": "cc_company", "type":"int"},
                  "cc_company_name":{"cf":"cc_company_name", "col": "cc_company_name", "type":"string"},
                  "cc_street_number":{"cf":"cc_street_number", "col": "cc_street_number", "type":"int"},
                  "cc_street_name":{"cf":"cc_street_name", "col": "cc_street_name", "type":"string"},
                  "cc_street_type":{"cf":"cc_street_type", "col": "cc_street_type", "type":"string"},
                  "cc_suite_number":{"cf":"cc_suite_number", "col": "cc_suite_number", "type":"string"},
                  "cc_city":{"cf":"cc_city", "col": "cc_city", "type":"string"},
                  "cc_county":{"cf":"cc_county", "col": "cc_county", "type":"string"},
                  "cc_state":{"cf":"cc_state", "col": "cc_state", "type":"string"},
                  "cc_zip":{"cf":"cc_zip", "col": "cc_zip", "type":"int"},
                  "cc_country":{"cf":"cc_country", "col": "cc_country", "type":"string"},
                  "cc_gmt_offset":{"cf":"cc_gmt_offset", "col": "cc_gmt_offset", "type":"int"},
                  "cc_tax_percentage":{"cf":"cc_tax_percentage", "col": "cc_tax_percentage", "type":"double"}
                  }
                 }""".stripMargin


val customer = s"""{
                  "table":{"namespace":"default", "name":"customer", "tableCoder":"PrimitiveType"},
                  "rowkey":"key",
                  "columns":{
                  "key":{"cf":"rowkey", "col":"key", "type":"int"},
                  "c_customer_sk":{"cf":"c_customer_sk", "col": "c_customer_sk", "type":"int"},
                  "c_customer_id":{"cf":"c_customer_id", "col": "c_customer_id", "type":"string"},
                  "c_current_cdemo_sk":{"cf":"c_current_cdemo_sk", "col": "c_current_cdemo_sk", "type":"int"},
                  "c_current_hdemo_sk":{"cf":"c_current_hdemo_sk", "col": "c_current_hdemo_sk", "type":"int"},
                  "c_current_addr_sk":{"cf":"c_current_addr_sk", "col": "c_current_addr_sk", "type":"int"},
                  "c_first_shipto_date_sk":{"cf":"c_first_shipto_date_sk", "col": "c_first_shipto_date_sk", "type":"int"},
                  "c_first_sales_date_sk":{"cf":"c_first_sales_date_sk", "col": "c_first_sales_date_sk", "type":"int"},
                  "c_salutation":{"cf":"c_salutation", "col": "c_salutation", "type":"string"},
                  "c_first_name":{"cf":"c_first_name", "col": "c_first_name", "type":"string"},
                  "c_last_name":{"cf":"c_last_name", "col": "c_last_name", "type":"string"},
                  "c_preferred_cust_flag":{"cf":"c_preferred_cust_flag", "col": "c_preferred_cust_flag", "type":"string"},
                  "c_birth_day":{"cf":"c_birth_day", "col": "c_birth_day", "type":"int"},
                  "c_birth_month":{"cf":"c_birth_month", "col": "c_birth_month", "type":"int"},
                  "c_birth_year":{"cf":"c_birth_year", "col": "c_birth_year", "type":"int"},
                  "c_birth_country":{"cf":"c_birth_country", "col": "c_birth_country", "type":"string"},
                  "c_login":{"cf":"c_login", "col": "c_login", "type":"string"},
                  "c_email_address":{"cf":"c_email_address", "col": "c_email_address", "type":"string"},
                  "c_last_review_date":{"cf":"c_last_review_date", "col": "c_last_review_date", "type":"int"}
                  }
                 }"""

val store = s"""{
                  "table":{"namespace":"default", "name":"store", "tableCoder":"PrimitiveType"},
                  "rowkey":"key",
                  "columns":{
                  "key":{"cf":"rowkey", "col":"key", "type":"int"},
                  "s_store_sk":{"cf":"s_store_sk", "col": "s_store_sk", "type":"int"},
                  "s_store_id":{"cf":"s_store_id", "col": "s_store_id", "type":"string"},
                  "s_rec_start_date":{"cf":"s_rec_start_date", "col": "s_rec_start_date", "type":"timestamp"},
                  "s_rec_end_date":{"cf":"s_rec_end_date", "col": "s_rec_end_date", "type":"timestamp"},
                  "s_closed_date_sk":{"cf":"s_closed_date_sk", "col": "s_closed_date_sk", "type":"int"},
                  "s_store_name":{"cf":"s_store_name", "col": "s_store_name", "type":"string"},
                  "s_number_employees":{"cf":"s_number_employees", "col": "s_number_employees", "type":"int"},
                  "s_floor_space":{"cf":"s_floor_space", "col": "s_floor_space", "type":"int"},
                  "s_hours":{"cf":"s_hours", "col": "s_hours", "type":"string"},
                  "s_manager":{"cf":"s_manager", "col": "s_manager", "type":"string"},
                  "s_market_id":{"cf":"s_market_id", "col": "s_market_id", "type":"int"},
                  "s_geography_class":{"cf":"s_geography_class", "col": "s_geography_class", "type":"string"},
                  "s_market_desc":{"cf":"s_market_desc", "col": "s_market_desc", "type":"string"},
                  "s_market_manager":{"cf":"s_market_manager", "col": "s_market_manager", "type":"string"},
                  "s_division_id":{"cf":"s_division_id", "col": "s_division_id", "type":"int"},
                  "s_division_name":{"cf":"s_division_name", "col": "s_division_name", "type":"string"},
                  "s_company_id":{"cf":"s_company_id", "col": "s_company_id", "type":"int"},
                  "s_company_name":{"cf":"s_company_name", "col": "s_company_name", "type":"string"},
                  "s_street_number":{"cf":"s_street_number", "col": "s_street_number", "type":"int"},
                  "s_street_name":{"cf":"s_street_name", "col": "s_street_name", "type":"string"},
                  "s_street_type":{"cf":"s_street_type", "col": "s_street_type", "type":"string"},
                  "s_suite_number":{"cf":"s_suite_number", "col": "s_suite_number", "type":"string"},
                  "s_city":{"cf":"s_city", "col": "s_city", "type":"string"},
                  "s_county":{"cf":"s_county", "col": "s_county", "type":"string"},
                  "s_state":{"cf":"s_state", "col": "s_state", "type":"string"},
                  "s_zip":{"cf":"s_zip", "col": "s_zip", "type":"int"},
                  "s_country":{"cf":"s_country", "col": "s_country", "type":"string"},
                  "s_gmt_offset":{"cf":"s_gmt_offset", "col": "s_gmt_offset", "type":"int"},
                  "s_tax_precentage":{"cf":"s_tax_precentage", "col": "s_tax_precentage", "type":"double"}
                  }
                 }"""

val store_returns = s"""{
                  "table":{"namespace":"default", "name":"store_returns", "tableCoder":"PrimitiveType"},
                  "rowkey":"key",
                  "columns":{
                  "key":{"cf":"rowkey", "col":"key", "type":"int"},
                  "sr_returned_date_sk":{"cf":"sr_returned_date_sk", "col": "sr_returned_date_sk", "type":"int"},
                  "sr_return_time_sk":{"cf":"sr_return_time_sk", "col": "sr_return_time_sk", "type":"int"},
                  "sr_item_sk":{"cf":"sr_item_sk", "col": "sr_item_sk", "type":"int"},
                  "sr_customer_sk":{"cf":"sr_customer_sk", "col": "sr_customer_sk", "type":"int"},
                  "sr_cdemo_sk":{"cf":"sr_cdemo_sk", "col": "sr_cdemo_sk", "type":"int"},
                  "sr_hdemo_sk":{"cf":"sr_hdemo_sk", "col": "sr_hdemo_sk", "type":"int"},
                  "sr_addr_sk":{"cf":"sr_addr_sk", "col": "sr_addr_sk", "type":"int"},
                  "sr_store_sk":{"cf":"sr_store_sk", "col": "sr_store_sk", "type":"int"},
                  "sr_reason_sk":{"cf":"sr_reason_sk", "col": "sr_reason_sk", "type":"int"},
                  "sr_ticket_number":{"cf":"sr_ticket_number", "col": "sr_ticket_number", "type":"int"},
                  "sr_return_quantity":{"cf":"sr_return_quantity", "col": "sr_return_quantity", "type":"int"},
                  "sr_return_amt":{"cf":"sr_return_amt", "col": "sr_return_amt", "type":"double"},
                  "sr_return_tax":{"cf":"sr_return_tax", "col": "sr_return_tax", "type":"double"},
                  "sr_return_amt_inc_tax":{"cf":"sr_return_amt_inc_tax", "col": "sr_return_amt_inc_tax", "type":"double"},
                  "sr_fee":{"cf":"sr_fee", "col": "sr_fee", "type":"double"},
                  "sr_return_ship_cost":{"cf":"sr_return_ship_cost", "col": "sr_return_ship_cost", "type":"double"},
                  "sr_refunded_cash":{"cf":"sr_refunded_cash", "col": "sr_refunded_cash", "type":"double"},
                  "sr_reversed_charge":{"cf":"sr_reversed_charge", "col": "sr_reversed_charge", "type":"double"},
                  "sr_store_credit":{"cf":"sr_store_credit", "col": "sr_store_credit", "type":"double"},
                  "sr_net_loss":{"cf":"sr_net_loss", "col": "sr_net_loss", "type":"double"}
                  }
                 }"""

val date_dim = s"""{
                  "table":{"namespace":"default", "name":"date_dim", "tableCoder":"PrimitiveType"},
                  "rowkey":"key",
                  "columns":{
                  "key":{"cf":"rowkey", "col":"key", "type":"int"},
                  "d_date_sk":{"cf":"d_date_sk", "col": "d_date_sk", "type":"int"},
                  "d_date_id":{"cf":"d_date_id", "col": "d_date_id", "type":"string"},
                  "d_date":{"cf":"d_date", "col": "d_date", "type":"timestamp"},
                  "d_month_seq":{"cf":"d_month_seq", "col": "d_month_seq", "type":"int"},
                  "d_week_seq":{"cf":"d_week_seq", "col": "d_week_seq", "type":"int"},
                  "d_quarter_seq":{"cf":"d_quarter_seq", "col": "d_quarter_seq", "type":"int"},
                  "d_year":{"cf":"d_year", "col": "d_year", "type":"int"},
                  "d_dow":{"cf":"d_dow", "col": "d_dow", "type":"int"},
                  "d_moy":{"cf":"d_moy", "col": "d_moy", "type":"int"},
                  "d_dom":{"cf":"d_dom", "col": "d_dom", "type":"int"},
                  "d_qoy":{"cf":"d_qoy", "col": "d_qoy", "type":"int"},
                  "d_fy_year":{"cf":"d_fy_year", "col": "d_fy_year", "type":"int"},
                  "d_fy_quarter_seq":{"cf":"d_fy_quarter_seq", "col": "d_fy_quarter_seq", "type":"int"},
                  "d_fy_week_seq":{"cf":"d_fy_week_seq", "col": "d_fy_week_seq", "type":"int"},
                  "d_day_name":{"cf":"d_day_name", "col": "d_day_name", "type":"string"},
                  "d_quarter_name":{"cf":"d_quarter_name", "col": "d_quarter_name", "type":"string"},
                  "d_holiday":{"cf":"d_holiday", "col": "d_holiday", "type":"string"},
                  "d_weekend":{"cf":"d_weekend", "col": "d_weekend", "type":"string"},
                  "d_following_holiday":{"cf":"d_following_holiday", "col": "d_following_holiday", "type":"string"},
                  "d_first_dom":{"cf":"d_first_dom", "col": "d_first_dom", "type":"int"},
                  "d_last_dom":{"cf":"d_last_dom", "col": "d_last_dom", "type":"int"},
                  "d_same_day_ly":{"cf":"d_same_day_ly", "col": "d_same_day_ly", "type":"int"},
                  "d_same_day_lq":{"cf":"d_same_day_lq", "col": "d_same_day_lq", "type":"int"},
                  "d_current_day":{"cf":"d_current_day", "col": "d_current_day", "type":"string"},
                  "d_current_week":{"cf":"d_current_week", "col": "d_current_week", "type":"string"},
                  "d_current_month":{"cf":"d_current_month", "col": "d_current_month", "type":"string"},
                  "d_current_quarter":{"cf":"d_current_quarter", "col": "d_current_quarter", "type":"string"},
                  "d_current_year":{"cf":"d_current_year", "col": "d_current_year", "type":"string"}
                  }
                 }"""

withCatalog(customer).createOrReplaceTempView("customer")
withCatalog(date_dim).createOrReplaceTempView("date_dim")
withCatalog(store).createOrReplaceTempView("store")
withCatalog(store_returns).createOrReplaceTempView("store_returns")

//sql("with customer_total_return as (select sr_customer_sk as ctr_customer_sk ,sr_store_sk as ctr_store_sk ,sum(SR_RETURN_AMT) as ctr_total_return from store_returns ,date_dim where sr_returned_date_sk = d_date_sk and d_year =2000 group by sr_customer_sk ,sr_store_sk)  select  c_customer_id from customer_total_return ctr1 ,store ,customer where ctr1.ctr_total_return > (select avg(ctr_total_return)*1.2 from customer_total_return ctr2 where ctr1.ctr_store_sk = ctr2.ctr_store_sk) and s_store_sk = ctr1.ctr_store_sk and s_state = 'TN' and ctr1.ctr_customer_sk = c_customer_sk order by c_customer_id  limit 100").show

