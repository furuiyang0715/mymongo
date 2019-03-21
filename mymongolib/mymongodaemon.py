import sys
import time
import logging
import os
import configparser

from importlib import util
from multiprocessing import Process
from multiprocessing import Queue
from apscheduler.schedulers.blocking import BlockingScheduler

from mymongolib.daemon import Daemon
from mymongolib import mysql
from mymongolib.mongodb import MyMongoDB
from mymongolib.datamunging import DataMunging
from mymongomodules.parse_data import ParseData
from mymongomodules.process_data import ProcessData

config = configparser.ConfigParser()
config.read('conf/config.ini')


class MyMongoDaemon(Daemon):
    """Subclass of :class:`.Daemon`

    """
    def run(self):
        """Runs the daemon

        Thims method runs the daemon and creates all the process needed. Then waits forever

        """
        # 配置日志文件作为标准 error 输出
        self.logger = logging.getLogger(__name__)
        sys.stderr = self.log_err

        # 给进程起一个名字 方便查看进程状态
        try:
            util.find_spec('setproctitle')
            self.setproctitle = True
            import setproctitle
            setproctitle.setproctitle('mymongo')
        except ImportError:
            self.setproctitle = False
    
        self.logger.info("Running")

        self.queues = dict()
        self.queues['replicator_out'] = Queue()

        procs = dict()  # 进程字典
        procs['scheduler'] = Process(name='scheduler', target=self.scheduler)
        procs['scheduler'].daemon = True
        procs['scheduler'].start()

        procs['replicator'] = Process(name='replicator', target=self.replicator)
        procs['replicator'].daemon = True
        procs['replicator'].start()

        procs['datamunging'] = Process(name='datamunging', target=self.data_munging)
        procs['datamunging'].daemon = True
        procs['datamunging'].start()

        procs['dataprocess'] = Process(name='dataprocess', target=self.data_process)
        procs['dataprocess'].daemon = True
        procs['dataprocess'].start()

        while True:
            self.logger.info('Working...')
            time.sleep(60)

    def scheduler(self):
        """Runs the daemon scheduler

        """
        # schedule 模块完成写入 pid 文件； 打印 schedule 启动日志
        self.write_pid(str(os.getpid()))
        if self.setproctitle:
            import setproctitle
            setproctitle.setproctitle('mymongo_scheduler')
        sched = BlockingScheduler()
        try:
            sched.add_job(self.dummy_sched, 'interval', minutes=1)
            sched.start()
        except Exception as e:
            self.logger.error('Cannot start scheduler. Error: ' + str(e))
    
    def dummy_sched(self):
        """Dummy method to keep the schedule running

        """
        # 保持 schedule 运行的一个虚拟方法： 不断打印日志
        self.logger.info('Scheduler works!')

    def write_pid(self, pid):
        """Write pid to the pidfile

        Args:
            pid (int): number of process id

        """
        # 将 pid 写入一个文件中
        open(self.pidfile, 'a+').write("{}\n".format(pid))

    def replicator(self):
        """Main process for replication. It writes entry in the replication queue

        See Also:
            :meth:`.data_munging`

        """
        # 复制的主要过程 完成： 在队列中写入

        # 将当前的进程号写入文件 这个文件中始终是当前正在运行的所有进程的 pid  a+的模式
        self.write_pid(str(os.getpid()))
        # 为当前的进程命名 方便查询
        if self.setproctitle:
            import setproctitle
            setproctitle.setproctitle('mymongo_replicator')

        mongo = MyMongoDB(config['mongodb'])
        # 将 binlog 数据写入的过程
        mysql.mysql_stream(config['mysql'], mongo, self.queues['replicator_out'])

    def data_munging(self):
        # 读取要插入的数据 并且写入mongo
        """Reads data from replpication queue and writes to mongo

        See Also:
            :meth:`.replicator`

        """
        # 标记进程并且填入文件
        self.write_pid(str(os.getpid()))
        if self.setproctitle:
            import setproctitle
            setproctitle.setproctitle('mymongo_datamunging')

        module_instance = ParseData()

        mongo = MyMongoDB(config['mongodb'])
        munging = DataMunging(mongo, self.queues['replicator_out'])
        # module_instance 的 run 是对数据的解析 暂时没有做解析 具体在 ParseData() 类中做处理
        munging.run(module_instance)

    def data_process(self):
        # 对数据进行处理的进程 读取---处理---写入
        # TODO(furuiyang) 这一步暂时没有在这里实现 后期将这里的代码转移
        self.write_pid(str(os.getpid()))
        if self.setproctitle:
            import setproctitle
            setproctitle.setproctitle('mymongo_dataprocess')

        mongo = MyMongoDB(config['mongodb'])
        process_instance = ProcessData(mongo)
        process_instance.run()
