import logging
import logging.handlers
import configparser
import sys
import time
import socket
import logging.config


from mymongolib import utils
from mymongolib.mongodb import MyMongoDB
from mymongolib.mymongodaemon import MyMongoDaemon
from mymongolib.utils import LoggerWriter


config = configparser.ConfigParser()
config.read('conf/config.ini')

logging.config.fileConfig('conf/logging.conf')
logger = logging.getLogger('root')

hostname = socket.gethostname()

if __name__ == '__main__':
    logger.info('begin mysql--> mongo process ......')

    # 命令行参数解析
    parser = utils.cmd_parser()

    args = parser.parse_args()
    mongo = MyMongoDB(config['mongodb'])
    if args.mysqldump_data:
        # 将 mysqldump_data 路径屏蔽
        sys.exit(0)
        # try:
        #     utils.run_mysqldump(dump_type='data', conf=config['mysql'], mongodb=mongo)
        #     logger.info('Data dump procedure ended')
        #     sys.exit(0)
        # except Exception as e:
        #     logger.error('Data dump procedure ended with errors: ' + str(e))
        #     sys.exit(1)
    elif args.mysqldump_schema:
        # 屏蔽 mysqldump_schema
        sys.exit(0)
        # try:
        #     utils.run_mysqldump(dump_type='schema', conf=config['mysql'], mongodb=mongo)
        #     logger.info('Schema dump procedure ended')
        #     sys.exit(0)
        # except Exception as e:
        #     logger.error('Schema dump procedure ended with errors: ' + str(e))
        #     sys.exit(1)
    elif args.mysqldump_complete:
        sys.exit(0)
        # try:
        #     utils.run_mysqldump(dump_type='complete', conf=config['mysql'], mongodb=mongo)
        #     logger.info('Complete dump procedure ended')
        #     sys.exit(0)
        # except Exception as e:
        #     logger.error('Complete dump procedure ended with errors: ' + str(e))
        #     sys.exit(1)
    if args.load_data:
        try:
            tables = eval(config['mysql']['table'])
            # print(tables)
            # print(type(tables))
            start = time.time()
            sec_list = utils.run_load_data(tables, conf1=config)
            # sec_list = []
            end = time.time()
            logger.info(f"The seccess table list is {sec_list}")
            logger.info(f"The time for this data-load is {(end-start)/60} min")
        except Exception as e:
            logger.error(f"complete load data ended with errors: {e}")

    log_err = LoggerWriter(logger, logging.ERROR)

    # 开启一个mongodb daemon instance
    # 参数：进程记录文件 and 日志
    mymongo_daemon = MyMongoDaemon(config['general']['pid_file'], log_err=log_err)

    # if args.start:
    #     for db in config['mysql']['databases'].split(','):
    #         parsed = mongo.get_db_as_parsed(db)
    #         if parsed is None:
    #             logger.error('Database schema ' + db + ' has not been parsed. Please run schema dump procedure before')
    #             sys.exit(1)
    #         elif parsed['schema'] == 'ko':
    #             logger.error('Database schema ' + db + ' has not been parsed. Please run schema dump procedure before')
    #             sys.exit(1)
    #         elif parsed['data'] == 'ko':
    #             logger.warning('Database data ' + db + ' has not been parsed. '
    #                                                    'It could be better to run data dump procedure before')

    # 开始增量同步之前对是否进行过 load_data 的校验
    if args.start:
        tables = eval(config['mysql']['already_table'])  # 可以进行增量更新的 table
        # print(tables)
        try:
            db_name = config['mongodb']['utildb']
            coll_name = config['mongodb']['parsed_table']
            coll = mongo.get_coll(coll_name, db_name)
            parsed = coll.find({"database": f"{config['mysql']['databases']}"}).next().get("parsed_table")
            # print(parsed)
        except Exception as e:
            logger.warning(f'fetch the parsed_table fail, the reason is {e}')
            sys.exit(1)

        if tables != parsed:
            logger.error(f"there are some tables that have not been loaded yet, they are {set(tables)-set(parsed)} ")
            sys.exit(1)

        mymongo_daemon.start()
    elif args.stop:
        mymongo_daemon.stop()
    elif args.restart:
        mymongo_daemon.restart()    # 在开启的状态下重启
    elif args.status:
        mymongo_daemon.status()
    sys.exit(0)

