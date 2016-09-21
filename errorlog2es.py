#-*- coding: utf-8 -*-

# Project   : Transfer RDS error log to elastic search.
# Author    : YW. Jang
# Date      : 2016.05.03
#
# Copyright 2016, YW. Jang, All rights reserved.

import boto3
import re
import os
import json
import time

from datetime import datetime
from datetime import timedelta
from dateutil import tz, zoneinfo

from elasticsearch import Elasticsearch

class ErrorlogSender:
  def __init__(self):
    self._ERRORLOG_PREFIX = "error/mysql-error-running.log."

    self._GENERAL_CONFIG = {
      # Elasticsearch host name
      "ES_HOST": "192.168.0.1:4040",
      
      # Elasticsearch prefix for index name
      "INDEX_PREFIX": "rds_errorlog",
      
      # Elasticsearch type name is rds instance id
      "RDS_ID": "tb-master",
      
      # Enabled to change timezone. If you set UTC, this parameter is blank
      "TIMEZONE": "Asia/Seoul",

      # RDS region which you want to crawling error log.
      "AWS_RDS_REGION_ID": "ap-northeast-2",

      # If you have ec2 instances, then It need region and VPC involving instances.
      "AWS_EC2_REGION_ID": "ap-northeast-2",
      "AWS_EC2_VPC_ID": "vpc-XXxxXXxx",
      }

    self._REGEX4REFINE = {
      "QUERYTIME_REGEX": re.compile("^[a-zA-Z#:_ ]+([0-9.]+)[a-zA-Z:_ ]+([0-9.]+)[a-zA-Z:_ ]+([0-9.]+).[a-zA-Z:_ ]+([0-9.]+)$"),
      "GENERAL_ERR": re.compile("(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) (\w+) \[(\w+)\] (.*)"),
      "ABORTED_CONN": re.compile("db: '(\w+)' user: '(\w+)' host: '([\w\d\.]+)'"),
      "ACCESS_DENY": re.compile("user '(.*)'@'([\d\.]+)' "),
      "DEADLOCK": re.compile("(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) (\w+)"),
      "ROLLBACK_TR": re.compile("\*\*\* WE ROLL BACK TRANSACTION \((\d+)\)"),
      "HOLD_USER_INFO": re.compile("MySQL thread id (\w+), OS thread handle \w+, query id (\d+) ([\d\.]+) (\w+) "),
      "HOLD_LOCK_INFO": re.compile("table `(\w+)`\.`(\w+)` trx id (\d+) lock_mode (\w+)")
      }

    self._ABORTED_CONN_MSG = "Aborted connection"
    self._ACCESS_DENY_MSG = "Access denied"

    self._BEGIN_DEADLOCK = "deadlock detected"
    self._END_DEADLOCK = "WE ROLL BACK TRANSACTION"
    self._BEGIN_TRX = "TRANSACTION"
    self._TRASACTION_LENGTH = 9

    self._es = Elasticsearch(self._GENERAL_CONFIG["ES_HOST"])
    self._ec2dict = dict()
    self._last_time = ""
    self._data = list()
    self._num_of_total_doc = 0

    self._now = datetime.now()

  def validateLogDate(self, lines):
    delta = timedelta(hours=2)
  
    for line in lines:
      if not line:
        continue
      elif line.startswith("# Time: "):
        log_time = datetime.strptime(line[8:], "%y%m%d %H:%M:%S")
        log_time = log_time.replace(tzinfo=tz.tzutc()).astimezone(zoneinfo.gettz(self._GENERAL_CONFIG["TIMEZONE"]))
        log_time = log_time.replace(tzinfo=None)
        print(self._now, log_time)
        print("diff :", self._now - log_time)
        if (self._now - log_time) > delta:
          return False
        else:
          return True
      
    return True
    
  def initElasticsearchIndex(self):
    self._ES_INDEX = self._GENERAL_CONFIG["INDEX_PREFIX"] + "-" + datetime.strftime(self._now, "%Y.%m")

  def initEC2InstancesInVpc(self, region, vpc):
    ec2 = boto3.resource("ec2", region_name=region)
    vpc = ec2.Vpc(vpc)
    for i in vpc.instances.all():
      for tag in i.tags:
        if tag['Key'] == 'Name':
          self._ec2dict[i.private_ip_address] = "".join(tag['Value'].split())

  def getRdsLog(self, log_filename):
    client = boto3.client("rds", region_name=self._GENERAL_CONFIG["AWS_RDS_REGION_ID"])
    db_files = client.describe_db_log_files(DBInstanceIdentifier=self._GENERAL_CONFIG["RDS_ID"])

    if not filter(lambda log: log["LogFileName"] == log_filename, db_files["DescribeDBLogFiles"]):
      return ""

    marker = "0"
    log_data = ""

    # It used like do-while statement.
    ret = client.download_db_log_file_portion(
        DBInstanceIdentifier=self._GENERAL_CONFIG["RDS_ID"],
        LogFileName=log_filename,
        Marker=marker,
        NumberOfLines=500)
    log_data = ret["LogFileData"]
    marker = ret["Marker"]

    while ret["AdditionalDataPending"]:
      ret = client.download_db_log_file_portion(
        DBInstanceIdentifier=self._GENERAL_CONFIG["RDS_ID"],
        LogFileName=log_filename,
        Marker=marker,
        NumberOfLines=500)

      log_data += ret["LogFileData"]
      marker = ret["Marker"]

    return log_data

  def getRdsLog4Debug(self, path):
    content = ""
    import codecs
    f = codecs.open(path, "r", "utf-8")
    while True:
      l = f.readline()
      content += l
      if not l: break
    return content

  def validate_log_date(self, line):
    delta = timedelta(hours=2)

    m = REG_GENERAL_ERR.match(line)
    if m:
      log_time = datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S")
      log_time = log_time.replace(tzinfo=tz.tzutc()).astimezone(zoneinfo.gettz(self._GENERAL_CONFIG["TIMEZONE"]))
      log_time = log_time.replace(tzinfo=None)
      if (self._now - log_time) > delta:
        return False
    elif BEGIN_DEADLOCK in line:
      m = REG_DEADLOCK.match(line)
      log_time = datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S")
      log_time = log_time.replace(tzinfo=tz.tzutc()).astimezone(zoneinfo.gettz(self._GENERAL_CONFIG["TIMEZONE"]))
      log_time = log_time.replace(tzinfo=None)
      if (self._now - log_time) > delta:
        return False

    return True

  def createTemplate(self, template_name):
    template_json = {
      "template" : "rds_errorlog-*",
      "settings" : {
        "number_of_shards": 1 }
    }

    response = self._es.indices.put_template(name=template_name, body=template_json)
    if response["acknowledged"]:
      print("Create template success.")
    else:
      print("Create template failed.")

  def appendDoc2Data(self, doc, flush=False):
    doc["timestamp"] = self._last_time
    self._data.append({"index": {
                         "_index": self._ES_INDEX,
                         "_type": self._GENERAL_CONFIG["RDS_ID"] }})
    self._data.append(doc)

    self._num_of_total_doc += 1

    
    if len(self._data) > 10000 or flush:
      self._es.bulk(index=self._ES_INDEX, body=self._data, refresh=flush)
      print("%s : Write doc into data that length is %s" % (str(datetime.now()), len(self._data)))

  def run(self):
    self.initElasticsearchIndex()
    log_filename = self._ERRORLOG_PREFIX + str((self._now).utcnow().hour)
    log_data = self.getRdsLog(log_filename)

    if not log_data:
      print("%s does not exist!" % (log_filename))
      return -1

    lines = log_data.split("\n")
    if len(lines) > 0:
      if not self.validateLogDate(lines):
        print("%s already read log!" % (log_filename))
        return -2
    else:
      print("%s is empty!" % (log_filename))
      return -3

    sself.initEC2InstancesInVpc(
      self._GENERAL_CONFIG["AWS_EC2_REGION_ID"],
      self._GENERAL_CONFIG["AWS_EC2_VPC_ID"])
    self.createTemplate(self._GENERAL_CONFIG["INDEX_PREFIX"])

    print("%s : Ready to write %s in %s" % (str(datetime.now()), log_filename, self._ES_INDEX))
    i = 0
    doc = {}

    while i < len(lines):
      line = lines[i]
      if not line:
        i += 1
        continue

      if doc:
        self.appendDoc2Data(doc)
        doc = {}

      doc = {}
      m = self._REGEX4REFINE["GENERAL_ERR"].match(line)
      if m:
        doc["type"] = "Errorlog"
        doc["code"] = m.group(2)
        doc["severity"] = m.group(3)

        # It need to be parse message additionally.
        # Specific cases as below.
        message = m.group(4)
        if self._ABORTED_CONN_MSG in message:
          doc["detail"] = self._ABORTED_CONN_MSG
          match = self._REGEX4REFINE["ABORTED_CONN"].search(message)
          doc["db"] = match.group(1)
          doc["user"] = match.group(2)
          doc["host"] = match.group(3)
          ip_addr = match.group(3)
          if ip_addr not in self._ec2dict:
            doc["name"] = "Missed"
          else:
            doc["name"] = self._ec2dict[ip_addr]
        elif self._ACCESS_DENY_MSG in message:
          doc["detail"] = self._ACCESS_DENY_MSG
          match = self._REGEX4REFINE["ACCESS_DENY"].search(message)
          doc["user"] = match.group(1)
          doc["host"] = match.group(2)
        else:
          doc["detail"] = "Other"
        doc["message"] = message

        timestamp = datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S")
        timestamp = timestamp.replace(tzinfo=tz.tzutc()).astimezone(zoneinfo.gettz(self._GENERAL_CONFIG["TIMEZONE"]))
        doc["timestamp"] = timestamp.isoformat()

      elif self._BEGIN_DEADLOCK in line:
        doc["type"] = "Deadlock"
        i += 1 # ignore deadlock dectected message
        m = self._REGEX4REFINE["DEADLOCK"].match(lines[i])

        timestamp = datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S")
        timestamp = timestamp.replace(tzinfo=tz.tzutc()).astimezone(zoneinfo.gettz(self._GENERAL_CONFIG["TIMEZONE"]))
        doc["timestamp"] = timestamp.isoformat()
        doc["code"] = m.group(2)
        i += 1 # get next line

        # This transaction wait for using the lock.
        tr_a = ""
        for offset in range(self._TRASACTION_LENGTH):
          tr_a += lines[i + offset] + "\n"
        i += self._TRASACTION_LENGTH
        doc["transaction_a"] = tr_a

        # Skip non-readable messages.
        while self._BEGIN_TRX not in lines[i]:
          i += 1

        # This transaction hold the lock.
        tr_b = ""
        for offset in range(self._TRASACTION_LENGTH):
          tr_b += lines[i + offset] + "\n"

        doc["transaction_b"] = tr_b

        m = self._REGEX4REFINE["HOLD_USER_INFO"].search(tr_b)
        doc["hold_lock_thread_id"] = m.group(1)
        doc["hold_lock_query_id"] = m.group(2)
        doc["hold_lock_usr"] = m.group(3)
        doc["hold_lock_ip"] = m.group(4)

        m = self._REGEX4REFINE["HOLD_LOCK_INFO"].search(tr_b)
        doc["hold_lock_db"] = m.group(1)
        doc["hold_lock_tb"] = m.group(2)
        doc["hold_lock_trx_id"] = m.group(3)
        doc["hold_lock_trx_mode"] = m.group(4)

        while self._END_DEADLOCK not in lines[i]:
          i += 1
        m = self._REGEX4REFINE["ROLLBACK_TR"].match(lines[i])
        rollback = ""
        if m.group(1) == "1": rollback = "a"
        else: rollback = "b"
        doc["rollback"] = rollback
      else:
        print("Parse Error at", i)
        doc["type"] = "Other"
        doc["message"] = line

      i += 1

    if doc:
      doc["timestamp"] = self._last_time
      print("%s : Write last data that length is %s (%s)" % (str(datetime.now()), len(self._data), len(doc)))
      self.appendDoc2Data(doc, flush=True)

    print("Written Errorlogs : %s" % str(self._num_of_total_doc))
    print("last_time : %s" % (self._last_time))

if __name__ == '__main__':
  el2es = ErrorlogSender()
  el2es.run()