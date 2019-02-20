import signal
import sys
import logging
import decimal
import datetime

from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
)


def mysql_stream(conf, mongo, queue_out):
    logger = logging.getLogger(__name__)

    # server_id is your slave identifier, it should be unique.
    # set blocking to True if you want to block and wait for the next event at
    # the end of the stream
    mysql_settings = {
        "host": conf['host'],
        "port": conf.getint('port'),
        "user": conf['user'],
        "passwd": conf['password']
    }

    last_log = mongo.get_log_pos()
    if last_log['log_file'] == 'NA':
        log_file = None
        log_pos = None
        resume_stream = False
    else:
        log_file = last_log['log_file']
        log_pos = int(last_log['log_pos'])
        resume_stream = True

    stream = BinLogStreamReader(connection_settings=mysql_settings,
                                server_id=conf.getint('slaveid'),
                                only_events=[DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent],
                                blocking=True,
                                resume_stream=resume_stream,
                                log_file=log_file,
                                log_pos=log_pos,
                                only_schemas=conf['databases'].split(','))

    for binlogevent in stream:
        binlogevent.dump()
        schema = "%s" % binlogevent.schema
        table = "%s" % binlogevent.table

        for row in binlogevent.rows:
            if isinstance(binlogevent, DeleteRowsEvent):
                vals = process_binlog_dict(row["values"])
                event_type = 'delete'
            elif isinstance(binlogevent, UpdateRowsEvent):
                vals = dict()
                vals["before"] = process_binlog_dict(row["before_values"])
                vals["after"] = process_binlog_dict(row["after_values"])
                event_type = 'update'
            elif isinstance(binlogevent, WriteRowsEvent):
                vals = process_binlog_dict(row["values"])
                event_type = 'insert'

            seqnum = mongo.write_to_queue(event_type, vals, schema, table)
            mongo.write_log_pos(stream.log_file, stream.log_pos)
            queue_out.put({'seqnum': seqnum})
            logger.debug(row)
            logger.debug(stream.log_pos)
            logger.debug(stream.log_file)

    stream.close()


def process_binlog_dict(_dict):
    # 针对从muysqlbin中解析出来的数据 进行插入之前的转换工作
    for k, v in _dict.items():
        if isinstance(v, decimal.Decimal):
            _dict.update({k: float(v)})
        elif isinstance(v, datetime.timedelta):
            _dict.update({k: str(v)})
        # (TODO 增加对 datetime 以及 datetime.delta 的数据格式校正

    return _dict
