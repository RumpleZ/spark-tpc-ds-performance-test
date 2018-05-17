import csv
import re
import sys
from collections import OrderedDict

sql2CassTypes = {'string':'text', 'datetime':'date'}


def getPrimaryKeys(tableName):
    d = {}
    d['store_sales'] = 'ss_item_sk, ss_ticket_number'
    d['store_returns'] = 'sr_item_sk, sr_ticket_number'
    d['catalog_sales'] = 'cs_item_sk, cs_order_number'
    d['catalog_returns'] = 'cr_item_sk, cr_order_number'
    d['web_sales'] = 'ws_item_sk, ws_order_number'
    d['web_returns'] = 'wr_item_sk, wr_order_number'
    d['inventory'] = 'inv_date_sk, inv_item_sk, inv_warehouse_sk'
    d['store'] = 's_store_sk'
    d['call_center'] = 'cc_call_center_sk'
    d['catalog_page'] = 'cp_catalog_page_sk'
    d['web_site'] = 'web_site_sk'
    d['web_page'] = 'wp_web_page_sk'
    d['warehouse'] = 'w_warehouse_sk'
    d['customer'] = 'c_customer_sk'
    d['customer_address'] = 'ca_address_sk'
    d['customer_demographics'] = 'cd_demo_sk'
    d['date_dim'] = 'd_date_sk'
    d['household_demographics'] = 'hd_demo_sk'
    d['item'] = 'i_item_sk'
    d['income_band'] = 'ib_income_band_sk'
    d['promotion'] = 'p_promo_sk'
    d['reason'] = 'r_reason_sk'
    d['ship_mode'] = 'sm_ship_mode_sk'
    d['time_dim'] = 't_time_sk'
    return d[tableName]




class CassandraTable:
    def __init__(self):
        self.namespace = ''
        self.tableName = ''
        self.createTable = ''
        self.fields = {}
        self.rows = []

    def typeConvertion(self, d):
        for key in d.keys():
            if d[key] in sql2CassTypes.keys():
                d[key] = sql2CassTypes[d[key]]
        return d

    def generateCreateTable(self, tableName, types):
        self.namespace = 'test'
        self.tableName = tableName
        temp_dict = OrderedDict()
        regex = re.compile('[^a-zA-Z]')
        for typ in types:
            if typ[0] not in ['(header', 'delimiter', 'path']:
                temp_dict[typ[0]] = regex.sub('',typ[1])
        temp_dict = self.typeConvertion(temp_dict)
        self.fields = temp_dict
        #print('\n')
        self.toSQLString()

    def toSQLString(self):
        regex = re.compile('[^a-zA-Z]')
        sql = "create table " + self.namespace + "." + self.tableName + "("
        for field in self.fields:
            sql += field + " " + regex.sub('', self.fields[field]) + ","
        sql += "primary key(" + getPrimaryKeys(self.tableName) + "));"
        self.createTable = sql
        self.loadRows()

        with open(tableName+'.lol', 'w') as fp:
            fp.write(self.createTable)
            for row in self.rows:
                fp.write(row)


    def loadRows(self):
        # tenho que por ' nas datas e string
        with open('gendata/'+self.tableName+'.dat', 'r') as csvfile:
            linereader = csv.reader(csvfile, delimiter='|')
            values = []
            keys = []
            i = 0
            auxList = list(self.fields.keys())
            for row in linereader:
                for item in row:
                    if item != '':
                        if self.fields[auxList[i]] in ['text', 'date']:
                            item = '\''+item.replace("'","''") +'\''
                        
                        values.append(item)
                        keys.append(auxList[i])
                    i+=1
                i = 0

                self.rows.append('insert into ' + self.namespace + "." + self.tableName + "("+','.join(keys) +
                                ") values (" + ','.join(values) + ");\n")
                keys = []; values = []

cassandraTables = []

buffer = ''
typeBuffer = []
sqls = []
with open("createTables.sql", "r") as sqlFile:
    for line in sqlFile.readlines():
        if line.startswith(';'):
            sqls.append(buffer)
            buffer = ''
        else:
            buffer += line.strip('\n')
    sqls.append(buffer)

p = re.compile('create table .*(?=_text)')
p1 = re.compile('(?:\().*?\)')
for i in sqls:
    tableName = p.match(i).group().split(' ')[2]
    for y in p1.findall(i):
        for z in y.split(','):
            typeBuffer.append(list(filter(lambda x: x is not '' and x is not '(', z.split(' '))))
    cassandraTables.append(CassandraTable().generateCreateTable(tableName,typeBuffer))
    typeBuffer = []
#x.generateCreateTable(buffer)

