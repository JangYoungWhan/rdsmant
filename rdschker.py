#-*- coding: utf-8 -*-

# Project   : Monitoring tool for RDS status when problem have been occured.
# Author    : YW. Jang
# Date      : 2016.05.03
#
# Copyright 2016, YW. Jang, All rights reserved.

import boto3
import yaml
#import mysql.connector
import subprocess

import logging
import logging.handlers

import datetime
import time
import timeit

import sys
import collections
import os # mkdir
import codecs # file R/W

class QueueKey:
  def __init__(self):
    self._RDS_ID = "RdsId"
    self._INNODB_STATUS = "inndbStatus"
    self._PROCESS_LIST = "processList"
    self._TIME_STAMP = "TimeStamp"

# DAOs...
class ShellCommander(QueueKey):
  def __init__(self, host, port, region, user, password):
    QueueKey.__init__(self)
    self._rds_id = ""

    self._host = host
    self._port = port
    self._region = region
    self._user = user
    self._passwrod = password

  def getRdsStatus(self):
    return dict({
      self._RDS_ID: self._rds_id,
      self._INNODB_STATUS: self.getInnodbStatus(),
      self._PROCESS_LIST: self.getProcessList(),
      self._TIME_STAMP: datetime.datetime.now().strftime("%Y%m%d%H%M%S")
      })

  def getInnodbStatus(self):
    query = "mysql -h %s -u %s -P %s -p%s -e 'SHOW ENGINE INNODB STATUS\\G'" % (self._host, self._user, self._port, self._passwrod)
    result = subprocess.check_output(query, stderr=subprocess.STDOUT, shell=True)
    return result

  def getProcessList(self):
    query = "mysql -h %s -u %s -P %s -p%s -e 'SHOW FULL PROCESSLIST'|grep -v 'Sleep'" % (self._host, self._user, self._port, self._passwrod)
    result =  subprocess.check_output(query, stderr=subprocess.STDOUT, shell=True)
    return result

class MySqlConnector(QueueKey):
  def __init__(self, host, port, region, user, password):
    QueueKey.__init__(self)
    self._db_config = {
      "user": user,
      "password": password,
      "host": host,
      "port": port,
      "raise_on_warnings": True,
      }

    self._rds_id = ""
    self._host = host
    self._port = port
    self._region = region
    self._user = user
    self._password = password

    # It need on python2.7, but does not on python3.4
    self._conn = mysql.connector.connect(**self._db_config)
    self._cursor = self._conn.cursor()

    self._INNODB_STATUS_QUERY = "SHOW ENGINE INNODB STATUS"
    self._PROCESS_LIST_QUERY = "SHOW FULL PROCESSLIST"

  def sendHeartBit(self):
    self._cursor.execute("SELECT 1")
    # just flush
    self._cursor.fetchall()

  def connect(self, logger):
    try:
      self._conn = mysql.connector.connect(**self._db_config)
      self._cursor = self._conn.cursor()
    except mysql.connector.Error as err:
      if err.errno == mysql.connector.errorcode.ER_ACCESS_DENIED_ERROR:
        logger.info("Something is wrong with your user name or password")
      elif err.errno == mysql.connector.errorcode.ER_BAD_DB_ERROR:
        logger.info("Database does not exist")
      else:
        logger.info(err)

  def getRdsStatus(self):
    return dict({
      self._RDS_ID: self._rds_id,
      self._INNODB_STATUS: self.getInnodbStatus(),
      self._PROCESS_LIST: self.getProcessList(),
      self._TIME_STAMP: datetime.datetime.now().strftime("%Y%m%d%H%M%S")
      })

  def getInnodbStatus(self):
    self._cursor.execute(self._INNODB_STATUS_QUERY)
    result = self._cursor.fetchall()

    innodb_status = ""
    if len(result) == 0:
      return innodb_status
    for i in result[0]:
      # In Python 2.7
      #innodb_status += unicode(i).encode("utf-8", "replace")
      innodb_status += i
      # In Python 3.4
      #innodb_status += str(i)

    return innodb_status

  def getProcessList(self):
    self._cursor.execute(self._PROCESS_LIST_QUERY)
    result = self._cursor.fetchall()

    result_str = ""
    for d in self._cursor.description:
      # In Python 2.7
      result_str += d[0] + "\t"
      #result_str += unicode(d[0]).encode("utf-8", "replace") + "\t"
      # In Python 3.4
      #result_str += str(d[0]) + "\t"
    result_str += "\n"

    for row in result:
      for i in row:
        # In Python 2.7
        result_str += i + "\t"
        # In Python 3.4
        #result_str += str(i) + "\t"
      result_str += "\n"

    return result_str

  def disconnect(self):
    self._conn.close()

class RdsMonitor(QueueKey):
  def __init__(self):
    QueueKey.__init__(self)
    self._watchers = list()
    self._thresholds = collections.defaultdict(lambda : collections.defaultdict(dict))
    self._status = collections.defaultdict(lambda : collections.defaultdict(dict))

    self._dao = list()

    self._target_metrics = {"DatabaseConnections":"DBC",
                            "DiskQueueDepth":"QDEP",
                            "ReadLatency":"READLAT",
                            "WriteLatency":"WRITELAT"}

    # attributes in rds-threshold.yml
    self._METRIC = "type"
    self._WARNING = "warning"

    # default values in monitor-config.yml
    self._interval = 30
    self._duration = 600
    self._out_path = ""

    self._LOG_EXT = ".log"

    # status queue
    self._rds_status_q = list()

    # logger
    self._rdslogger = RdsMonLogger()

  def readYaml(self, input):
    with open(input, "r") as f:
      return yaml.load(f)

  def loadRdsMonConfig(self, input):
    config = self.readYaml(input)

    # read keys in monitor-config.yml
    for i in config["dao"]:
      dao = ShellCommander(i["host"], i["port"], i["region"], i["user"], i["password"])
      dao._rds_id = (i["host"])[:i["host"].index(".")]
      self._dao.append(dao)

    #for i in config["condition"]:
    self._interval = config["condition"]["interval"]
    self._duration = config["condition"]["duration"]

    self._out_path = config["output"]["path"]
    if self._out_path[-1] != "/":
      self._out_path += "/";

    #for i in self._dao:
      #i.connect(self._rdslogger._logger)

    # Set log configuration
    path = config["logger"]["path"]
    filename = config["logger"]["filename"]
    encoding = config["logger"]["encoding"]
    maxbytes = config["logger"]["maxbytes"]
    backupcount = config["logger"]["backupcount"]

    self._rdslogger.loadConfig(path, filename, encoding, maxbytes, backupcount)

  def testHealthCheck(self):
    for dao in self._dao:
      dao.sendHeartBit()

  def enequeueRdsStatus(self):
    results = list()
    for i in self._dao:
      results.append(i.getRdsStatus())

    self._rds_status_q.append(results)

  def makeThresholdTable(self, input):
    config = self.readYaml(input)

    for ins in config["default"]:
      m = ins[self._METRIC]
      self._thresholds["default"][m] = ins[self._WARNING]

    for c in self._dao:
      ins = c._rds_id
      if ins in config:
        for t in config[ins]:
          m = t[self._METRIC]
          self._thresholds[ins][m] = t[self._WARNING]

  def isRdsStable(self):
    for c in self._dao:
      ins = c._rds_id
      for m in self._target_metrics.keys():
        abb = self._target_metrics[m]

        if self._status[ins][m] > self._thresholds[ins][abb]:
          self._rdslogger._logger.warn("Unstable : %s > %s",self._status[ins][m], self._thresholds[ins][abb])
          return False
    return True

  def writeAllInQueue(self):
    for i in self._rds_status_q:
      self.writeFile(i)

  def writeLatestInQueue(self):
    self.writeFile(self._rds_status_q[len(self._rds_status_q) - 1])

  def writeFile(self, status):
    for s in status:
      cur_date = self._out_path + (s[self._TIME_STAMP])[0:8]
      if not os.path.exists(cur_date):
        os.makedirs(cur_date)

      output_prefix = (s[self._TIME_STAMP])[8:] + "_" + s[self._RDS_ID]
      innodb_stat_path = cur_date + "/" + output_prefix + "_" + self._INNODB_STATUS + self._LOG_EXT
      ps_path = cur_date + "/" + output_prefix + "_" + self._PROCESS_LIST + self._LOG_EXT

      self.writeStatus(innodb_stat_path, s[self._INNODB_STATUS])
      self.writeStatus(ps_path, s[self._PROCESS_LIST])

  def writeStatus(self, file_name, content):
    out_file = codecs.open(file_name, "wb")
    out_file.write(content)
    out_file.close()
    self._rdslogger._logger.info("Write : %s", file_name)

  def run(self):
    for i in self._dao:
      for m in self._target_metrics.keys():
        w = CloudWatcher(i._region, i._rds_id, m)
        w.loadRegion(i._region)
        self._watchers.append(w)

    self._rdslogger._logger.info("==================================")
    self._rdslogger._logger.info("===  RDS Monitor get started!  ===")
    self._rdslogger._logger.info("==================================")

    expiry_date = datetime.datetime.min
    while True:
      start = timeit.default_timer()

      # Test output files.
      #isRdsStable = True

      # Apply reality.
      for w in self._watchers:
        w.watch(self._status, self._rdslogger._logger)

      isRdsStable = self.isRdsStable()
      # enqueue
      self.enequeueRdsStatus()

      if not isRdsStable:
        if datetime.datetime.now() > expiry_date: # First occured
          self.writeAllInQueue()
          self._rdslogger._logger.info("RDS is stable, but I found a problem at first.")
        else: # It have been occured consistently
          self.writeLatestInQueue()
          self._rdslogger._logger.info("It have been occured consistently.")
        expiry_date = datetime.datetime.now() + datetime.timedelta(seconds = self._duration)
      else:
        if datetime.datetime.now() <= expiry_date:
          self.writeLatestInQueue()
          self._rdslogger._logger.info("I wanna log it due the expiry date.")
        else:
          self._rdslogger._logger.info("Rds is currently stable.")

      # dequeue
      if len(self._rds_status_q) > (self._duration / self._interval):
        self._rds_status_q.pop(0)

      # Test output files.
      #self.writeLatestInQueue()

      duration = timeit.default_timer() - start
      self._rdslogger._logger.info("Time Spent : %s sec", duration)
      self._rdslogger._logger.info("Sleep : %s sec", self._interval)
      time.sleep(self._interval)

      # You have to consider wait_timeout prameter in RDS, because it have to smaller than sleep time.
      #self.testHealthCheck()

class RdsMonLogger:
  def __init__(self):
    self._logger = logging.getLogger('RDS_MON_LOGGER')
    self._logger.setLevel(logging.INFO)

  def loadConfig(self, logpath, fname, enc, bytes, backup):
    if not os.path.exists(logpath):
        os.makedirs(logpath)

    if logpath[-1] != "/":
      logpath += "/"
    path = logpath + fname

    # create console handler and set level to debug
    handlers = [
        logging.handlers.RotatingFileHandler(filename=path, encoding=enc, maxBytes=int(bytes), backupCount=int(backup)),
        logging.StreamHandler()
      ]

    # create formatter
    formatter = logging.Formatter('%(asctime)s %(name)s [%(levelname)s] : %(message)s')

    for h in handlers:
      h.setLevel(logging.INFO)
      h.setFormatter(formatter)
      self._logger.addHandler(h)

class CloudWatcher():
  def __init__(self, region, ins_name, metric):
    self._clients = dict()
    self._status = collections.defaultdict(lambda : collections.defaultdict(dict))

    self._client = boto3.client("cloudwatch", region_name = region)
    self._region = region
    self._ins_name = ins_name
    self._metric = metric

  def loadRegion(self, r):
    self._clients[r] = boto3.client("cloudwatch", region_name = r)

  def watch(self, status, logger):
    response = self._client.get_metric_statistics(
      Namespace = "AWS/RDS",
      MetricName = self._metric,
      Dimensions = [
                     {
                       "Name": "DBInstanceIdentifier",
                       "Value": self._ins_name,
                     },
                   ],
      StartTime = datetime.datetime.utcnow() - datetime.timedelta(minutes=2),
      EndTime = datetime.datetime.utcnow(),
      Period = 60,
      Statistics = ["Maximum"]
      )

    if len(response["Datapoints"]) > 0:
      status[self._ins_name][self._metric] = response["Datapoints"][0]["Maximum"]
      if self._metric.endswith("Latency"):
          status[self._ins_name][self._metric] *= float(1000)
      cur_stat = self._ins_name + "[" + self._metric + "]=" + str(status[self._ins_name][self._metric])
      logger.info(cur_stat)
      print(cur_stat)
      #logger.info("%s[%s]=%0.5f", self._ins_name, self._metric, status[self._ins_name][self._metric])
    else:
      status[self._ins_name][self._metric] = -1

if __name__ == "__main__":
  mon = RdsMonitor()

  mon.loadRdsMonConfig("./monitor-config.yml")
  mon.makeThresholdTable("./rds-threshold.yml")

  mon.run()