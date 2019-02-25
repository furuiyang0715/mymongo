# # -*- coding: utf-8 -*-
import re
import datetime
from lxml import etree

import configparser
import sys
import socket
import logging.config


from mymongolib import utils
from mymongolib.mongodb import MyMongoDB

config = configparser.ConfigParser()
config.read('conf/config.ini')

logging.config.fileConfig('conf/logging.conf')
logger = logging.getLogger('root')

hostname = socket.gethostname()


class SysException(Exception):
    """Custom exception class

    Args:
        *args: the same as exception class
        **kwargs: the same as exception class

    """
    def __init__(self, *args, **kwargs):
        Exception.__init__(self, *args, **kwargs)


def mysqldump_parser_schema(dump_file, mongodb):
    inputbuffer = ''
    db_start = re.compile(r'.*<database name.*', re.IGNORECASE)
    tb_start = re.compile(r'.*<table_structure.*', re.IGNORECASE)
    tb_end = re.compile(r'.*</table_structure.*', re.IGNORECASE)
    db = ''
    table = ''

    with open(dump_file, 'r') as inputfile:
        print("inputfile: ", inputfile)
        append = False
        for line in inputfile:
            print("the line is :", line)
            if tb_start.match(line):
                print('start')
                sys.exit(0)
                inputbuffer = line
                append = True
                table = re.findall('name="(.*?)"', line, re.DOTALL)[0]
            elif tb_end.match(line):
                print('end')
                inputbuffer += line
                append = False
                print("begin inputbuffer")
                process_schema_buffer(inputbuffer, table, db, mongodb)
                inputbuffer = None
                del inputbuffer
            elif append:
                print('elif')
                inputbuffer += line
            elif db_start.match(line):
                db = re.findall('name="(.*?)"', line, re.DOTALL)[0]

    try:
        mongodb.make_db_as_parsed(db, 'schema')
    except Exception as e:
        print('Cannot insert db ' + db + ' as parsed')


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
        print("-------------------------------")
        print(e)

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
        print('Cannot insert db ' + db + ' as parsed')


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
                if child.text.startswith("0"):
                    doc[child.attrib['name']] = child.text[1:]

    try:
        mongodb.insert(doc, db, table)
        print("wwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwww插入成功wwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwww")
    except Exception as e:
        print(e)

    del tnode


def run_mysqldump(dump_type, conf, mongodb):
    for db in ['datacenter']:
        try:
            dump_file = "/home/furuiyang/async/d2.sql"
        except Exception as e:
            raise SysException(e)

        if dump_type == 'data':
            try:
                print("---type=data---")
                mysqldump_parser_data(dump_file, mongodb)
            except Exception as e:
                raise SysException(e)

        if dump_type == 'schema':
            try:
                print("---type=schema---")
                mysqldump_parser_schema(dump_file, mongodb)
            except Exception as e:
                raise SysException(e)

        if dump_type == 'complete':
            try:
                print("---type=complete---")
                mysqldump_parser_schema(dump_file, mongodb)
            except Exception as e:
                print("###################################")
                print(e)

            try:
                mysqldump_parser_data(dump_file, mongodb)
            except Exception as e:
                raise SysException(e)

    return True


if __name__ == '__main__':
    print('Start mymongo')
    mongo = MyMongoDB(config['mongodb'])
    try:
        run_mysqldump(dump_type='data', conf=config['mysql'], mongodb=mongo)
        print('Complete dump procedure ended')
        sys.exit(0)
    except Exception as e:
        print('Complete dump procedure ended with errors: ' + str(e))
        sys.exit(1)
