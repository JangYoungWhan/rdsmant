#-*- coding: utf-8 -*-

# Project   : Transfer RDS slowquery log to elastic search.
# Author    : YW. Jang
# Date      : 2016.07.06
#
# Copyright 2016, YW. Jang, All rights reserved.

import boto3
import glob
import os
import re
import time

from datetime import date
from datetime import datetime
from datetime import timedelta
from dateutil import tz, zoneinfo

from elasticsearch import Elasticsearch

class SlowquerySender:
  def __init__(self):
    self._SLOWQUERYLOG_PREFIX = "slowquery/mysql-slowquery.log."

    self._GENERAL_CONFIG = {
      # Elasticsearch host name
      "ES_HOST": "192.168.0.1:4040",
      
      # Elasticsearch prefix for index name
      "INDEX_PREFIX": "rds_slowquery",
      
      # Elasticsearch type name is rds instance id
      "RDS_ID": "tb-master",
      
      # Enabled to change timezone. If you set UTC, this parameter is blank
      "TIMEZONE": "Asia/Seoul",

      # RDS region which you want to crawling error log.
      "AWS_RDS_REGION_ID": "ap-northeast-2",

      # If you have ec2 instances, then It need region and VPC involving instances.
      "AWS_EC2_REGION_ID": "ap-northeast-2",
      "AWS_EC2_VPC_ID": "vpc-XXxxXXxx"
      }

    self._REGEX4REFINE = {
      "REG_TIME": re.compile("^[a-zA-Z#:_ ]+([0-9.]+)[a-zA-Z:_ ]+([0-9.]+)[a-zA-Z:_ ]+([0-9.]+).[a-zA-Z:_ ]+([0-9.]+)$"),
      }

    self._LOG_CONFIG = {
      "LOG_OUTPUT_DIR": "/var/log/rdslog/slowquery2es.log",
      "RAW_OUTPUT_DIR": "/var/log/rdslog/slowquery" # (Optional)
      }

    self._es = Elasticsearch(self._GENERAL_CONFIG["ES_HOST"])
    self._ec2dict = dict()
    self._last_time = ""
    self._data = list()
    self._new_doc = True
    self._num_of_total_doc = 0
    self._now = datetime.now()

    self._reaminer = RawFileRemainer(self._LOG_CONFIG["RAW_OUTPUT_DIR"])

  # Get raw data.
  def getRdsSlowQlog(self, log_filename):
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
      print("keep going...")

      log_data += ret["LogFileData"]
      marker = ret["Marker"]

    # Delete old log files.
    self._reaminer.clearOutOfDateRawFiles()
    self._reaminer.makeRawLog("mysql-slowquery.log." + str((datetime.now().utcnow()).hour), log_data)

    return log_data

  def getRdsSlowQlog4Debug(self, path):
    content = ""
    import codecs
    f = codecs.open(path, "r", "utf-8")
    while True:
      l = f.readline()
      content += l
      if not l: break
    return content

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

  # Initialization.
  def initLastTime(self, path):
    if not os.path.exists(path):
      cur_time = dself._now.strftime("%y%m%d %H:%M:%S")
      self._last_time = datetime.strptime(cur_time, "%y%m%d %H:%M:%S").isoformat()
      return False

    last_re = re.compile("last_time : (.*)\\n?")

    ifs = open(path, "r")
    lines = ifs.readlines()

    for l in reversed(lines):
      if not l: continue
      m = last_re.match(l)
      if m is not None:
        self._last_time = (m.groups(0))[0]
        ifs.close()
        return True
    ifs.close()

    cur_time = self._now.strftime("%y%m%d %H:%M:%S")
    self._last_time = datetime.strptime(cur_time, "%y%m%d %H:%M:%S").isoformat()
    return False

  def initEC2InstancesInVpc(self, region, vpc):
    for attempt in range(3):
      try:
        ec2 = boto3.resource("ec2", region_name=region)
        vpc = ec2.Vpc(vpc)
        for i in vpc.instances.all():
          for tag in i.tags:
            if tag['Key'] == 'Name':
              self._ec2dict[i.private_ip_address] = "".join(tag['Value'].split())
      except:
        time.sleep(3)
        print("sleeping..., because DescribeInstances have been failed.")
      else:
        break

  def setTargetIndex(self):
    self._ES_INDEX = self._GENERAL_CONFIG["INDEX_PREFIX"] + "-" + datetime.strftime(self._now, "%Y.%m")

  def createTemplate(self, template_name):
    template_body = {
      "template" : "test_slowquerylog-*",
      "mappings" : {
        self._GENERAL_CONFIG["RDS_ID"]: {
          "properties": {
            "query_time": {
              "type": "float",
              "index": "not_analyzed" },
            "row_sent": {
              "type": "integer",
              "index": "not_analyzed" },
            "rows_examined": {
              "type": "integer",
              "index": "not_analyzed" },
            "lock_time": {
              "type": "float",
              "index": "not_analyzed" }
            }
          }
        },
      "settings" : {
        "number_of_shards": 1,
        "number_of_replicas": 0 }
    }

    response = self._es.indices.put_template(name=template_name, body=template_body)
    if response["acknowledged"]:
      print("Create template success.")
    else:
      print("Create template failed.")

  def isNewDoc(self, line):
    if (self._new_doc) and (line.startswith("# Time: ") or line.startswith("# User@Host: ")):
      return True
    else:
      return False

  def refreshLastTime(self, line):
    timestamp = datetime.strptime(line[8:], "%y%m%d %H:%M:%S")
    if self._GENERAL_CONFIG["TIMEZONE"]:
      timestamp = timestamp.replace(tzinfo=tz.tzutc()).astimezone(zoneinfo.gettz(self._GENERAL_CONFIG["TIMEZONE"]))
    self._last_time = timestamp.isoformat()

  def removeDuplicatedLineFeed(self, s):
    stripped = s.strip()
    # Substitue multiple line feed to single line feed.
    stripped = re.sub(r"(\n)+", r"\n", stripped)
    return stripped

  def appendDoc2Data(self, doc, flush=False):
    doc["timestamp"] = self._last_time
    doc["sql"] = self.removeDuplicatedLineFeed(doc["sql"])
    self._data.append({"index": {
                         "_index": self._ES_INDEX,
                         "_type": self._GENERAL_CONFIG["RDS_ID"] }})
    self._data.append(doc)

    self._num_of_total_doc += 1
    if len(self._data) > 100000 or flush:
      print("I'll gonna send~!")
      self._es.bulk(index=self._ES_INDEX, body=self._data, refresh=flush)

  def initNewDoc(self, doc, l1, l2, i):
    if l1.startswith("# Time: "):
      self.refreshLastTime(l1)
      doc["timestamp"] = self._last_time      
      i += 2 # Because we use two lines above.
    elif l1.startswith("# User@Host: "):
      doc["timestamp"] = self._last_time
      l2 = l1
      i += 1
    else:
      doc["timestamp"] = self._last_time
      print("There is an another pattern!")

    doc["user"] = l2.split("[")[1].split("]")[0]
    doc["client"] = l2.split("[")[2].split("]")[0]
    doc["client_id"] = l2.split(" Id: ")[1]
    ip_addr = doc["client"]
    if ip_addr not in self._ec2dict:
      doc["name"] = "Missed"
    else:
      doc["name"] = self._ec2dict[ip_addr]

    self._new_doc = False

    return doc, i

  def run(self):
    log_filename = self._SLOWQUERYLOG_PREFIX + str((self._now.utcnow()).hour)
    log_data = self.getRdsSlowQlog(log_filename)

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
    
    # Get ready for extracting log file.
    self.initLastTime(self._LOG_CONFIG["LOG_OUTPUT_DIR"])
    self.initEC2InstancesInVpc(
      self._GENERAL_CONFIG["AWS_EC2_REGION_ID"],
      self._GENERAL_CONFIG["AWS_EC2_VPC_ID"])
    self.setTargetIndex()
    self.createTemplate(self._GENERAL_CONFIG["INDEX_PREFIX"])
    
    print("%s : Ready to write %s in %s" % (str(datetime.now()), log_filename, self._ES_INDEX))
    i = 0
    doc = {}

    # Consider a case when only new one is appeared.
    while not self.isNewDoc(lines[i]):
      i += 1
      continue

    while i < len(lines):
      line = lines[i]

      if self.isNewDoc(line):
        if doc:
          self.appendDoc2Data(doc)
          doc = {}
        
        doc, i = self.initNewDoc(doc, lines[i], lines[i+1], i)
        line = lines[i]

      if line.startswith("# Query_time: "):
        m = self._REGEX4REFINE["REG_TIME"].match(line).groups(0)
        doc["query_time"] = m[0]
        doc["lock_time"] = m[1]
        doc["rows_sent"] = m[2]
        doc["rows_examined"] = m[3]
      else:
        if doc.get("sql"):
          doc["sql"] += "\n" + line
        else:
          doc["sql"] = line
        self._new_doc = True

      i += 1

    if doc:
      self.appendDoc2Data(doc, flush=True)

    print("Written Slow Queries : %s" % str(self._num_of_total_doc))
    print("last_time : %s" % (self._last_time))

class DirectoryManager:
  def __init__(self, path="/var/log"):
    self._raw_path = path

  def readInputPath(self, input_path):
    input_files = list()

    if os.path.isfile(input_path):
      self.input_files.append(input_path)
    else:
      for (dirpath, dirnames, filelist) in os.walk(input_path):
        for fname in filelist:
          self.input_files.extend(fname)
    return input_files

  def readDatePath(self, date_path):
    date_file_list = glob.glob(date_path + "/[0-9]*/[0-9]*/[0-9]*")
    date_path_list = filter(lambda d: os.path.isdir(d), date_file_list)

    return map(self.regularizePath, date_path_list)

  def regularizePath(self, s):
    return re.sub("[\\\|/]+", "/", s)

  def mkdir(self, path):
    if not os.path.exists(path):
      os.makedirs(path)

  def rmdir(self, path):
    # It prevents from deleting all your disk files.
    if path == "/" or not os.path.exists(path):
      return False

    for root, dirs, files in os.walk(path, topdown=False):
      for name in files:
        print('remove: ' + os.path.join(root, name))
        os.remove(os.path.join(root, name))
      for name in dirs:
        print('removedir: ' + os.path.join(root, name))
        os.removedirs(os.path.join(root, name))
    return True

  def isEmptyDir(self, dir):
    if os.path.exists(dir) and os.listdir(dir) == []:
      return True
    else:
      return False

class RawFileRemainer(DirectoryManager):
  def __init__(self, path):
    DirectoryManager.__init__(self, path)
    self._due_date = timedelta(weeks=2)

  def clearOutOfDateRawFiles(self):
    target = self.readDatePath(self._raw_path)

    for t in target:
      sp = t.split("/")
      if self.isOutOfDate(int(sp[-3]), int(sp[-2]), int(sp[-1])):
        print("delete : " + t)
        self.rmdirRecursively(t)

  def isOutOfDate(self, year, month, day):
    target = date(year, month, day)
    if (date.today() - target) > self._due_date:
      return True
    else:
      return False

  def rmdirRecursively(self, target_dir):
    self.rmdir(target_dir)

    if self.isEmptyDir(target_dir):
      print("removedirs!: " + target_dir)
      os.removedirs(target_dir)
    month_dir = target_dir[:-3]
    if self.isEmptyDir(month_dir):
      print("isEmptyDir!: " + month_dir)
      os.removedirs(month_dir)
    year_dir = target_dir[:-6]
    if self.isEmptyDir(year_dir):
      print("isEmptyDir!: " + year_dir)
      os.removedirs(year_dir)

  def makeRawLog(self, name, raw_data):
    cur_date_path = self._raw_path + self._now.strftime("/%Y/%m/%d/")
    raw_name = datetime.now().strftime("%Hh%Mm%Ss_") + name

    if not os.path.exists(cur_date_path):
      self.mkdir(cur_date_path)
    raw_path = cur_date_path + raw_name
    f = open(raw_path, "w")
    f.write(raw_data)
    f.close()
    print("%s : Remain raw log data : %s" % (str(datetime.now()), raw_path))


if __name__ == '__main__':
  sq2es = SlowquerySender()
  try:
    sq2es.run()
  except Exception as e:
    print(e)
