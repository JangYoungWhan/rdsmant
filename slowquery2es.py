#-*- coding: utf-8 -*-

# Project   : Transfer RDS slowquery log to elastic search.
# Author    : YW. Jang
# Date      : 2016.07.06
#
# Copyright 2016, YW. Jang, All rights reserved.

import boto3
import json
import subprocess
import glob
import os
import re
import time

from datetime import date
from datetime import datetime
from datetime import timedelta
from dateutil import tz, zoneinfo

from botocore.awsrequest import AWSRequest
from botocore.auth import SigV4Auth
from botocore.endpoint import PreserveAuthSession
from botocore.credentials import Credentials

class SlowquerySender:
  def __init__(self):
    self._SLOWQUERYLOG_PREFIX = "slowquery/mysql-slowquery.log."

    self._GENERAL_CONFIG = {
      # Elasticsearch host name
      "ES_HOST": "127.0.0.1:8088",
      
      # Elasticsearch prefix for index name
      "INDEX_PREFIX": "rds_slowquerylog",
      
      # DB instance identifier
      "RDS_ID": "", ## ex ) master
      
      # Enabled to change timezone. If you set UTC, this parameter is blank
      "TIMEZONE": "Asia/Seoul",

      # RDS region which you want to crawling error log.
      "AWS_RDS_REGION_ID": "", ## ex) us-west-2

      # If you have ec2 instances, then It need region and VPC involving instances.
      "AWS_EC2_REGION_ID": "", ## ex) us-west-2
      "AWS_EC2_VPC_ID": "", ## ex) vpc-123abc456

      # Stay empty the fingerprint path, if you don't want to use percona Fingerprint tool.
      # (Optional)
      "FINGERPRINT_EXCUTABLE": "" ## ex) /bin/pt-Fingerprint
      }

    self._CREDENTIALS = {
      "AWS_ACCESS_KEY_ID": os.environ["AWS_ACCESS_KEY_ID"],
      "AWS_SECRET_ACCESS_KEY": os.environ["AWS_SECRET_ACCESS_KEY"]
      }

    self._REGEX4REFINE = {
      "REG_TIME": re.compile("^[a-zA-Z#:_ ]+([0-9.]+)[a-zA-Z:_ ]+([0-9.]+)[a-zA-Z:_ ]+([0-9.]+).[a-zA-Z:_ ]+([0-9.]+)$"),
      }

    self._LOG_CONFIG = {
      "LOG_OUTPUT_DIR": "/var/log/rdsmon/slowquery2es.log",
      "RAW_OUTPUT_DIR": "" # (Optional) ## ex) /home/rds/raw
      }

    self._log_filename = "NONE"
    self._ec2dict = dict()
    self._last_time = ""
    self._data = ""
    self._new_doc = True
    self._num_of_total_doc = 0

    self._fp = Fingerprinter(self._GENERAL_CONFIG["FINGERPRINT_EXCUTABLE"])
    self._reaminer = RawFileRemainer(self._LOG_CONFIG["RAW_OUTPUT_DIR"])

  # Get raw data.
  def getRdsSlowQlog(self, now):
    client = boto3.client("rds", region_name=self._GENERAL_CONFIG["AWS_RDS_REGION_ID"])
    db_files = client.describe_db_log_files(DBInstanceIdentifier=self._GENERAL_CONFIG["RDS_ID"])

    self._log_filename = self._SLOWQUERYLOG_PREFIX + str(now.utcnow().hour)
    if not filter(lambda log: log["LogFileName"] == self._log_filename, db_files["DescribeDBLogFiles"]):
      return ""

    marker = "0"
    log_data = ""

    # It used like do-while statement.
    ret = client.download_db_log_file_portion(
        DBInstanceIdentifier=self._GENERAL_CONFIG["RDS_ID"],
        LogFileName=self._log_filename,
        Marker=marker,
        NumberOfLines=500)
    log_data = ret["LogFileData"]
    marker = ret["Marker"]

    while ret["AdditionalDataPending"]:
      ret = client.download_db_log_file_portion(
        DBInstanceIdentifier=self._GENERAL_CONFIG["RDS_ID"],
        LogFileName=self._log_filename,
        Marker=marker,
        NumberOfLines=500)

      log_data += ret["LogFileData"]
      marker = ret["Marker"]

    # Delete old log files.
    self._reaminer.clearOutOfDateRawFiles()

    self._reaminer.makeRawLog("mysql-slowquery.log." + str(now.utcnow().hour), log_data)

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

  def validateLogDate(self, now, lines):
    delta = timedelta(hours=2)
  
    for line in lines:
      if not line:
        continue
      elif line.startswith("# Time: "):
        log_time = datetime.strptime(line[8:], "%y%m%d %H:%M:%S")
        log_time = log_time.replace(tzinfo=tz.tzutc()).astimezone(zoneinfo.gettz(self._GENERAL_CONFIG["TIMEZONE"]))
        log_time = log_time.replace(tzinfo=None)
        print(now, log_time)
        print("diff :", now - log_time)
        if (now - log_time) > delta:
          return False
        else:
          return True
      
    return True

  # Initialization.
  def initLastTime(self, path):
    if not os.path.exists(path):
      cur_time = datetime.now().strftime("%y%m%d %H:%M:%S")
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

    cur_time = datetime.now().strftime("%y%m%d %H:%M:%S")
    self._last_time = datetime.strptime(cur_time, "%y%m%d %H:%M:%S").isoformat()
    return False

  def initEC2InstancesInVpc(self, region, vpc):
    for attempt in range(3):
      try:
        ec2 = boto3.resource("ec2", region_name=region)
        vpc = ec2.Vpc(vpc)
      except:
        time.sleep(3)
      else:
        for i in vpc.instances.all():
          for tag in i.tags:
            if tag['Key'] == 'Name':
              self._ec2dict[i.private_ip_address] = "".join(tag['Value'].split())
        break

  def getCredentials(self):
    return Credentials(self._CREDENTIALS["AWS_ACCESS_KEY_ID"],
                       self._CREDENTIALS["AWS_SECRET_ACCESS_KEY"])

  def setTargetIndex(self, now):
    self._ES_INDEX = self._GENERAL_CONFIG["INDEX_PREFIX"] + "-" + datetime.strftime(now, "%Y.%m")

  def createTemplate(self, host):
    d = dict()
    d["template"] = "rds_slowquerylog-*"
    d["settings"] = dict()
    d["settings"]["number_of_shards"] = 1
    d["mappings"] = dict()
    d["mappings"][self._GENERAL_CONFIG["RDS_ID"]] = dict()
    d["mappings"][self._GENERAL_CONFIG["RDS_ID"]]["properties"] = dict()
    d["mappings"][self._GENERAL_CONFIG["RDS_ID"]]["properties"]["query_time"] = {"type": "float", "index": "not_analyzed"}
    d["mappings"][self._GENERAL_CONFIG["RDS_ID"]]["properties"]["lock_time"] = {"type": "float", "index": "not_analyzed"}
    d["mappings"][self._GENERAL_CONFIG["RDS_ID"]]["properties"]["rows_sent"] = {"type": "integer", "index": "not_analyzed"}
    d["mappings"][self._GENERAL_CONFIG["RDS_ID"]]["properties"]["rows_examined"] = {"type": "integer", "index": "not_analyzed"}

    credentials = self.getCredentials()
    url = self.createURL(host, "/_template/rds_slowquerylog?ignore_conflicts=true")
    response = self.request2ES(url, "PUT", credentials, data=json.dumps(d))
    if not response.ok:
      print(response.text)


  # Methods to sending data to ElasticSearch.
  def createURL(self, host, path, ssl=False):
    if not path.startswith("/"):
      path = "/" + path

    if ssl:
      return "https://" + host + path
    else:
      return "http://" + host + path

  def request(self, url, method, credentials, service_name, region=None, headers=None, data=None):
    if not region:
      region = "us-west-1"

    aws_request = AWSRequest(url=url, method=method, headers=headers, data=data)
    SigV4Auth(credentials, service_name, region).add_auth(aws_request)
    return PreserveAuthSession().send(aws_request.prepare())

  def request2ES(self, url, method, credentials, region=None, headers=None, data=None):
    return self.request(url, method, credentials, "es", region, headers, data)

  def send2ES(self):
    credentials = self.getCredentials()
    url = self.createURL(self._GENERAL_CONFIG["ES_HOST"], "/_bulk")
    response = self.request2ES(url, "POST", credentials, data=self._data)
    
    if not response.ok:
      print(response.text)
    print("%s : Write doc into data that length is %s" % (str(datetime.now()), len(self._data)))
    self._data = ""

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
    doc["fingerprint"] = self._fp.regularizeFingerprint(doc["fingerprint"])
    self._data += '{"index":{"_index":"' + self._ES_INDEX + '","_type":"' + self._GENERAL_CONFIG["RDS_ID"] + '"}}\n'        
    self._data += json.dumps(doc) + "\n"

    self._num_of_total_doc += 1
    if len(self._data) > 100000 or flush:
      self.send2ES()

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
    now = datetime.now()
    log_data = self.getRdsSlowQlog(now)
    #log_data = self.getRdsSlowQlog4Debug("D:/Downloads/mysql-slowquery.log.1")

    if not log_data:
      print("%s does not exist!" % (self._log_filename))
      return -1

    lines = log_data.split("\n")
    if len(lines) > 0:
      if not self.validateLogDate(now, lines):
        print("%s already read log!" % (self._log_filename))
        return -2
    else:
      print("%s is empty!" % (self._log_filename))
      return -3
    
    # Get ready for extracting log file.
    self.initLastTime(self._LOG_CONFIG["LOG_OUTPUT_DIR"])
    self.initEC2InstancesInVpc(
      self._GENERAL_CONFIG["AWS_EC2_REGION_ID"],
      self._GENERAL_CONFIG["AWS_EC2_VPC_ID"])
    self.setTargetIndex(now)
    self.createTemplate(self._GENERAL_CONFIG["ES_HOST"])
    
    print("%s : Ready to write %s in %s" % (str(datetime.now()), self._log_filename, self._ES_INDEX))
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
          doc["fingerprint"] += "\n" + self._fp.abstractQuery(line)
        else:
          doc["sql"] = line
          doc["fingerprint"] = self._fp.abstractQuery(line)
        self._new_doc = True

      i += 1

    if doc:
      self.appendDoc2Data(doc, flush=True)

    print("Written Slow Queries : %s" % str(self._num_of_total_doc))
    print("last_time : %s" % (self._last_time))

class Fingerprinter():
  def __init__(self, fp_path):
    self._SET_TIMESTAMP = "set timestamp="
    self._USE_DATABASE = "use polariscloud"
    self._fp_path = fp_path

  def skipFingerprint(self, query):
    if query.startswith(self._SET_TIMESTAMP):
      return ""
    elif query.startswith(self._USE_DATABASE):
      return ""
    else:
      return query

  def abstractQuery(self, query):
    if not self._fp_path:
      return query

    cmd = "%s --query '%s'" % (_fp_path, query)

    try:
      return self.skipFingerprint(subprocess.check_output(cmd, shell=True))
    except:
      return query

  def regularizeFingerprint(self, query):
    SET_TIMESTAMP = "set timestamp=?;\n\n"
    USE_DATABASE = "use ?\n\n"
  
    if SET_TIMESTAMP in query:
      query = query.replace(SET_TIMESTAMP, "")
    if USE_DATABASE in query:
      query = query.replace(USE_DATABASE, "")
    
    # Substitue multiple line feed to single line feed.
    query = re.sub(r"(\n)+", r"\n", query)
    query = s.strip()
  
    return query

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
    cur_date_path = self._raw_path + datetime.now().strftime("/%Y/%m/%d/")
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
  sq2es.run()