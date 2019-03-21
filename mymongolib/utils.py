import argparse
import logging
import subprocess
import datetime
import os
import re
import time
import csv

from tempfile import NamedTemporaryFile
from lxml import etree

from mymongolib.mongodb import MyMongoDB
from .exceptions import SysException


logger = logging.getLogger(__name__)


def cmd_parser():
    parser = argparse.ArgumentParser(description='Replicate a MySQL database to MongoDB')
    # parser.add_argument('--resume-from-end', dest='resume_from_end',
    #                    action='store_true', help="Even if the binlog\
    #                    replication was interrupted, start from the end of\
    #                    the current binlog rather than resuming from the interruption",
    #                    default=False)
    # parser.add_argument('--resume-from-start', dest='resume_from_start',
    #                    action='store_true', help="Start from the beginning\
    #                    of the current binlog, regardless of the current position", default=False)
    # parser.add_argument('--mysqldump-file', dest='mysqldump_file', type=str,
    #                    help='Specify a file to get the mysqldump from, rather\
    #                    than having ditto running mysqldump itself',
    #                    default='')

    parser.add_argument('--mysqldump-schema', dest='mysqldump_schema',
                        action='store_true', help="Run mysqldump to create new databases on mongodb, but \
                        not import any data so you can review mmongodb schema before importing data", default=False)
    parser.add_argument('--mysqldump-data', dest='mysqldump_data',
                        action='store_true', help="Run mysqldump to import only data", default=False)
    parser.add_argument('--mysqldump-complete', dest='mysqldump_complete',
                        action='store_true', help="Run mysqldump to import schema and data", default=False)

    parser.add_argument('--start', dest='start',
                        action='store_true', help="Start the daemon process", default=False)
    parser.add_argument('--stop', dest='stop',
                        action='store_true', help="Stop the daemon process", default=False)
    parser.add_argument('--restart', dest='restart',
                        action='store_true', help="Restart the daemon process", default=False)
    parser.add_argument('--status', dest='status',
                        action='store_true', help="Status of the daemon process", default=False)

    parser.add_argument('--load-data', dest='load_data',
                        action='store_true', help="dump csv file from mysql and import it to mongo", default=False)
    return parser


def read_txt(file):
    try:
        with open(file, "r") as f:
            '''
            for example:
            ['File',
             'Position',
             'Binlog_Do_DB',
             'Binlog_Ignore_DB',
             'Executed_Gtid_Set',
             'bin.000025',
             '250160467',
             'e9d7d07e-d01a-11e8-91f4-fa163e442f54:1-121496']
            '''
            ll = f.read().split()
            binfile = ll[5]
            position = ll[6]
            # print(binfile)
            # print(position)
    except Exception as e:
        logger.info(f"get file and pos form {file} fail, the reason is {e}")
        tt = time.time()
        return tt, tt
    return binfile, position


def mysqlcsv_cmd(table, conf):
    showposcommand = f"mysql -h {conf['host']} -u{conf['user']} -p{conf['password']} -e 'select * from {conf['databases']}.{table};'"
    # print(showposcommand)

    temp_file = conf["txt_dir"] + f"data__{table}.txt"
    with open(temp_file, "w") as f:
        try:
            p1 = subprocess.Popen(showposcommand, shell=True, stdout=f)
        except Exception as e:
            raise SystemError(e)
    p1.wait()
    return temp_file


def txt2csv(file):
    w_file = open(file.split(".")[0]+".csv", "w")
    # print(w_file)
    writer_file = csv.writer(w_file)

    try:
        with open(file) as f:
            f_csv = csv.reader(f)
            headers = next(f_csv)
            # 处理表头
            writer_file.writerow(headers[0].split())
            # 处理 row
            for row in f_csv:
                writer_file.writerow(row[0].split("\t"))
    except Exception as e:
        raise SystemError(e)
    finally:
        pass
        # 关闭写入流
        #  writer_file.close()
    return w_file.name


def import2mongo(file, table, conf):
    # mongoimport --host=127.0.0.1 --db datacenter --collection table1 --type csv --headerline --ignoreBlanks --file table1.csv
    showposcommand = f"mongoimport --host={conf['host']} --db {conf['databases']} --collection {table} --type csv --headerline --ignoreBlanks --file {file}"
    # print(showposcommand)

    # drop collection if exist...
    mongo = MyMongoDB(conf)
    try:
        mongo.drop_coll(conf['databases'], table)
    except Exception:
        pass

    try:
        p1 = subprocess.Popen(showposcommand, shell=True)
    except Exception as e:
        logger.warning(f"fail to import to mongodb, because {e}")
        # raise SystemError(e)
        return None
    p1.wait()
    return True


def run_load_data(tables, conf1):
    sec_list = list()
    conf = conf1["mysql"]
    for table in tables:
        logger.info(f"begin load data from table {table}")
        # 步骤1 ： 生成同步前的记录文件
        temp_file_1 = mysqlinfo_cmd(table, conf)
        # logger.info(temp_file_1)

        # 步骤2： 记录同步前的 file 和 pos
        file1, pos1 = read_txt(temp_file_1)
        # logger.info(file1, pos1)

        # 步骤3： 导出 txt 格式数据
        txt_file = mysqlcsv_cmd(table, conf)
        # print(txt_file)

        # 步骤4： 生成同步之后的记录文件
        temp_file_2 = mysqlinfo_cmd(table, conf)

        # 步骤5： 记录同步后的 file 和 pos
        file2, pos2 = read_txt(temp_file_2)

        # 步骤6：判断是否一致
        if file1 == file2 and pos1 == pos2:
            # 步骤7：将 txt 转换为 csv
            csv_file = txt2csv(txt_file)
            # print(csv_file)

            # 步骤8： 导入 CVS --> mongodb
            if import2mongo(csv_file, table, conf1["mongodb"]):
                # 步骤9: 将文件和位置信息写入 util 数据库

                # 步骤10： 导入成功 生成列表
                sec_list.append(table)

    return sec_list


def mysqlinfo_cmd(table, conf):
    showposcommand = f"mysql -h {conf['host']} -u{conf['user']} -p{conf['password']} -e 'show master status;'"
    # print(showposcommand)

    temp_file = conf["txt_dir"] + f"{table}.txt"
    with open(temp_file, "w") as f:
        try:
            p1 = subprocess.Popen(showposcommand, shell=True, stdout=f)
        except Exception as e:
            raise SystemError(e)
    p1.wait()
    return temp_file


def run_mysqldump(dump_type, conf, mongodb):
    for db in conf['databases'].split(','):
        try:
            dump_file = mysqldump_cmd(conf, db, dump_type=dump_type)
            # dump_file = "/home/furuiyang/async/d2.sql"
        except Exception as e:
            raise SysException(e)

        if dump_type == 'data':
            try:
                mysqldump_parser_data(dump_file, mongodb)
            except Exception as e:
                raise SysException(e)

        if dump_type == 'schema':
            try:
                mysqldump_parser_schema(dump_file, mongodb)
            except Exception as e:
                raise SysException(e)

        if dump_type == 'complete':
            try:
                mysqldump_parser_schema(dump_file, mongodb)
            except Exception as e:
                raise SysException(e)

            try:
                mysqldump_parser_data(dump_file, mongodb)
            except Exception as e:
                raise SysException(e)

    return True


def process_data_buffer(buf, table, db, mongodb):
    parser = etree.XMLParser(recover=True)
    tnode = etree.fromstring(buf, parser=parser)
    doc = dict()
    for child in tnode:
        if child.tag == 'field':
            doc[child.attrib['name']] = child.text

            key = mongodb.get_type_info(table, db)
            type_info = key.get("types", dict())

            if child.text and type_info.get(child.attrib['name']) == "int":
                doc[child.attrib['name']] = int(child.text)

            elif child.text and type_info.get(child.attrib['name']) == "datetime":
                _format = "%Y-%m-%d %H:%M:%S"
                _new = datetime.datetime.strptime(child.text, _format)
                doc[child.attrib['name']] = _new

            elif child.text and type_info.get(child.attrib['name']) == "decimal":
                doc[child.attrib['name']] = float(child.text)

            elif child.text and type_info.get(child.attrib['name']) == "float":
                doc[child.attrib['name']] = float(child.text)

            # elif child.text and type_info.get(child.attrib['name']) == "longblob":
            #     pass

            elif child.text and type_info.get(child.attrib['name']) == "time":
                # "09:00:00" --> datetime.timedelta --> "9:00:00"
                # "09:00:00" --> "9:00:00"
                if child.text.startswith("0"):
                    doc[child.attrib['name']] = child.text[1:]

    try:
        mongodb.insert(doc, db, table)
    except Exception as e:
        raise SysException(e)

    del tnode


def process_schema_buffer(buf, table, db, mongodb):
    parser = etree.XMLParser(recover=True)
    tnode = etree.fromstring(buf, parser=parser)
    doc = dict()
    doc['_id'] = db + '.' + table
    doc['primary_key'] = []
    doc['table'] = table
    doc['db'] = db
    doc["types"] = dict()
    for child in tnode:
        if child.tag == 'field':
            if child.attrib['Key'] == 'PRI':
                doc['primary_key'].append(child.attrib['Field'])

            if "int" in child.attrib['Type']:
                doc["types"].update({child.attrib['Field']: "int"})
            elif 'datetime' in child.attrib['Type']:
                doc["types"].update({child.attrib['Field']: "datetime"})
            elif "decimal" in child.attrib['Type']:
                doc["types"].update({child.attrib['Field']: "decimal"})
            elif "longblob" in child.attrib["Type"]:
                doc["types"].update({child.attrib['Field']: "longblob"})
            elif "float" in child.attrib["Type"]:
                doc["types"].update({child.attrib['Field']: "float"})
            elif "time" in child.attrib["Type"]:
                doc["types"].update({child.attrib['Field']: "time"})

    try:
        mongodb.insert_primary_key(doc)
    except Exception as e:
        raise SysException(e)

    del tnode


def mysqldump_parser_data(dump_file, mongodb):
    inputbuffer = ''
    db_start = re.compile(r'.*<database name.*', re.IGNORECASE)
    tb_start = re.compile(r'.*<table_data.*', re.IGNORECASE)
    row_start = re.compile(r'.*<row>.*', re.IGNORECASE)
    row_end = re.compile(r'.*</row>.*', re.IGNORECASE)
    master_log = re.compile(r'.*CHANGE MASTER.*', re.IGNORECASE)
    db = ''
    table = ''
    log_file = None
    log_pos = None

    with open(dump_file, 'r') as inputfile:
        append = False
        for line in inputfile:
            if row_start.match(line):
                # print('start')
                inputbuffer = line
                append = True
            elif row_end.match(line):
                # print('end')
                inputbuffer += line
                append = False
                process_data_buffer(inputbuffer, table, db, mongodb)
                inputbuffer = None
                del inputbuffer
            elif append:
                # print('elif')
                inputbuffer += line
            elif db_start.match(line):
                db = re.findall('name="(.*?)"', line, re.DOTALL)[0]
                try:
                    mongodb.drop_db(db)
                except Exception as e:
                    raise SysException(e)
            elif tb_start.match(line):
                table = re.findall('name="(.*?)"', line, re.DOTALL)[0]
            elif master_log.match(line):
                log_file = re.findall("MASTER_LOG_FILE='(.*?)'", line, re.DOTALL)[0]
                log_pos = re.findall("MASTER_LOG_POS=(.*?);", line, re.DOTALL)[0]

    if log_file is not None and log_pos is not None:
        try:
            mongodb.write_log_pos(log_file, log_pos)
        except Exception as e:
            raise SysException(e)

    try:
        mongodb.make_db_as_parsed(db, 'data')
    except Exception as e:
        logger.error('Cannot insert db ' + db + ' as parsed')


def mysqldump_parser_schema(dump_file, mongodb):
    inputbuffer = ''
    db_start = re.compile(r'.*<database name.*', re.IGNORECASE)
    tb_start = re.compile(r'.*<table_structure.*', re.IGNORECASE)
    tb_end = re.compile(r'.*</table_structure.*', re.IGNORECASE)
    db = ''
    table = ''

    with open(dump_file, 'r') as inputfile:
        append = False
        for line in inputfile:
            if tb_start.match(line):
                # print('start')
                inputbuffer = line
                append = True
                table = re.findall('name="(.*?)"', line, re.DOTALL)[0]
            elif tb_end.match(line):
                # print('end')
                inputbuffer += line
                append = False
                process_schema_buffer(inputbuffer, table, db, mongodb)
                inputbuffer = None
                del inputbuffer
            elif append:
                # print('elif')
                inputbuffer += line
            elif db_start.match(line):
                db = re.findall('name="(.*?)"', line, re.DOTALL)[0]

    try:
        mongodb.make_db_as_parsed(db, 'schema')
    except Exception as e:
        logger.error('Cannot insert db ' + db + ' as parsed')
    # TODO add index from mysql schema


def mysqldump_cmd(conf, db, dump_type):
    dump_file = NamedTemporaryFile(delete=False)
    dumpcommand = ['mysqldump',
                    '--user=' + conf['user'],
                    '--host=' + conf['host'],
                    '--port=' + conf['port'],
                    '--force',
                    '--xml',
                    "--force",
                    "--skip-opt",
                    "--single-transaction",
                    '--master-data=2',
                    "--default-character-set=utf8",
                   ]
    if conf['password'] != '':
        dumpcommand.append('--password=' + conf['password'])
    if dump_type == 'schema':
        dumpcommand.append('--no-data')
    elif dump_type == 'data':
        dumpcommand.append('--no-create-db')
        dumpcommand.append('--no-create-info')
    dumpcommand.append(db)

    logger.debug('executing: {0}'.format(' '.join(dumpcommand)))

    with open(dump_file.name, 'wb', 0) as f:
        try:
            p1 = subprocess.Popen(dumpcommand, stdout=f)
        except Exception as e:
            raise SysException(e)
    p1.wait()

    return dump_file.name

    # TODO save log_pos to mongo to start from here with replicator


class LoggerWriter:
    def __init__(self, logger, level):
        self.logger = logger
        self.level = level

    def write(self, message):
        if message != '\n':
            self.logger.log(self.level, message)

    def flush(self):
        return True
