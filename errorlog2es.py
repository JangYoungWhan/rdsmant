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
from datetime import datetime
from datetime import timedelta
from dateutil import tz, zoneinfo
from botocore.awsrequest import AWSRequest
from botocore.auth import SigV4Auth
from botocore.endpoint import PreserveAuthSession
from botocore.credentials import Credentials

# Elasticsearch host name and default region.
ES_HOST = "192.168.0.1:4040"
ES_DEFAULT_REGION = "us-west-1"

# Elasticsearch prefix for index name.
INDEX_PREFIX = "errorlog"

# Elasticsearch type name is rds instance id.
RDS_ID = "tb-master"

# Enabled to change timezone.  If you set UTC, this parameter is blank.
TIMEZONE = "Asia/Seoul"

# Query time format regex.
TIME_REGEX = "^[a-zA-Z#:_ ]+([0-9.]+)[a-zA-Z:_ ]+([0-9.]+)[a-zA-Z:_ ]+([0-9.]+).[a-zA-Z:_ ]+([0-9.]+)$"

R = re.compile(TIME_REGEX)
NOW = datetime.now()
INDEX = INDEX_PREFIX + "-" + datetime.strftime(NOW, "%Y.%m")
TYPE = RDS_ID
ERRORLOG_PREFIX = "error/mysql-error-running.log."

ABORTED_CONN_MSG = "Aborted connection"
ACCESS_DENY_MSG = "Access denied"

BEGIN_DEADLOCK = "deadlock detected"
END_DEADLOCK = "WE ROLL BACK TRANSACTION"
BEGIN_TRX = "TRANSACTION"

TRASACTION_LENGTH = 9

REG_GENERAL_ERR = re.compile("(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) (\w+) \[(\w+)\] (.*)")
REG_ABORTED_CONN = re.compile("db: '(\w+)' user: '(\w+)' host: '([\w\d\.]+)'")
REG_ACCESS_DENY = re.compile("user '(.*)'@'([\d\.]+)' ")
REG_DEADLOCK = re.compile("(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) (\w+)")
REG_ROLLBACK_TR = re.compile("\*\*\* WE ROLL BACK TRANSACTION \((\d+)\)")
REG_HOLD_USER_INFO = re.compile("MySQL thread id (\w+), OS thread handle \w+, query id (\d+) ([\d\.]+) (\w+) ")
REG_HOLD_LOCK_INFO = re.compile("table `(\w+)`\.`(\w+)` trx id (\d+) lock_mode (\w+)")

# RDS region which you want to crawling error log.
AWS_RDS_REGION_ID = "us-west-1"

# If you have ec2 instances, then It need region and VPC involving instances.
AWS_EC2_REGION_ID = "us-west-1"
AWS_EC2_VPC_ID = "vpc-xxxxx"


def lambda_handler():#(event, context):
  print("%s : Run errorlog2es.py" % (str(datetime.now())))
  client = boto3.client("rds", region_name=AWS_RDS_REGION_ID)
  db_files = client.describe_db_log_files(DBInstanceIdentifier=RDS_ID)

  log_filename = ERRORLOG_PREFIX + str(datetime.utcnow().hour)
  if not filter(lambda log: log["LogFileName"] == log_filename, db_files["DescribeDBLogFiles"]):
    print("%s does not exist!" % (log_filename))
    return

  marker = "0"
  log_data = ""

    # It used like do-while statement.
  ret = client.download_db_log_file_portion(
        DBInstanceIdentifier=RDS_ID,
        LogFileName=log_filename,
        Marker=marker,
        NumberOfLines=500)
  log_data = ret["LogFileData"]
  marker = ret["Marker"]

  while ret["AdditionalDataPending"]:
    ret = client.download_db_log_file_portion(
          DBInstanceIdentifier=RDS_ID,
          LogFileName=log_filename,
          Marker=marker,
          NumberOfLines=500)

    log_data += ret["LogFileData"]
    marker = ret["Marker"]

  data = ""
  doc = {}

  lines = log_data.split("\n")
  if len(lines) > 0:
    if not _validate_log_date(NOW, lines[0]):
      print("%s already read log before!" % (log_filename))
      return
  else:
    print("%s is empty!" % (log_filename))
    return

  # Get ready for extracting log file.
  i = 0
  ec2list = getEC2InstancesInVpc(AWS_EC2_REGION_ID, AWS_EC2_VPC_ID)
  _create_index(ES_HOST)
  print("%s : Write %s in %s" % (str(datetime.now()), log_filename, INDEX))

  while i < len(lines):
    line = lines[i]
    if not line:
      i += 1
      continue

    if doc:
      data += '{"index":{"_index":"' + INDEX + '","_type":"' + TYPE + '"}}\n'
      data += json.dumps(doc) + "\n"
    if len(data) > 100000:
      _bulk(ES_HOST, data)
      print("%s : Write data that length is %s" % (str(datetime.now()), len(data)))
      data = ""

    doc = {}
    m = REG_GENERAL_ERR.match(line)
    if m:
      doc["type"] = "Errorlog"
      doc["code"] = m.group(2)
      doc["severity"] = m.group(3)

      # It need to be parse message additionally.
      # Specific cases as below.
      message = m.group(4)
      if ABORTED_CONN_MSG in message:
        doc["detail"] = ABORTED_CONN_MSG
        match = REG_ABORTED_CONN.search(message)
        doc["db"] = match.group(1)
        doc["user"] = match.group(2)
        doc["host"] = match.group(3)
        ip_addr = match.group(3)
        if ip_addr not in ec2list:
          doc["name"] = "Missed"
        else:
          doc["name"] = ec2list[ip_addr]
      elif ACCESS_DENY_MSG in message:
        doc["detail"] = ACCESS_DENY_MSG
        match = REG_ACCESS_DENY.search(message)
        doc["user"] = match.group(1)
        doc["host"] = match.group(2)
      else:
        doc["detail"] = "Other"
      doc["message"] = message

      timestamp = datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S")
      timestamp = timestamp.replace(tzinfo=tz.tzutc()).astimezone(zoneinfo.gettz(TIMEZONE))
      doc["timestamp"] = timestamp.isoformat()

    elif BEGIN_DEADLOCK in line:
      doc["type"] = "Deadlock"
      i += 1 # ignore deadlock dectected message
      m = REG_DEADLOCK.match(lines[i])

      timestamp = datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S")
      timestamp = timestamp.replace(tzinfo=tz.tzutc()).astimezone(zoneinfo.gettz(TIMEZONE))
      doc["time"] = timestamp.isoformat()
      doc["code"] = m.group(2)
      i += 1 # get next line

      # This transaction wait for using the lock.
      tr_a = ""
      for offset in range(TRASACTION_LENGTH):
        tr_a += lines[i + offset] + "\n"
      i += TRASACTION_LENGTH
      doc["transaction_a"] = tr_a

      # Skip non-readable messages.
      while BEGIN_TRX not in lines[i]:
        i += 1

      # This transaction hold the lock.
      tr_b = ""
      for offset in range(TRASACTION_LENGTH):
        tr_b += lines[i + offset] + "\n"

      doc["transaction_b"] = tr_b

      m = REG_HOLD_USER_INFO.search(tr_b)
      doc["hold_lock_thread_id"] = m.group(1)
      doc["hold_lock_query_id"] = m.group(2)
      doc["hold_lock_usr"] = m.group(3)
      doc["hold_lock_ip"] = m.group(4)

      m = REG_HOLD_LOCK_INFO.search(tr_b)
      doc["hold_lock_db"] = m.group(1)
      doc["hold_lock_tb"] = m.group(2)
      doc["hold_lock_trx_id"] = m.group(3)
      doc["hold_lock_trx_mode"] = m.group(4)

      while END_DEADLOCK not in lines[i]:
        i += 1
      m = REG_ROLLBACK_TR.match(lines[i])
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
    data += '{"index":{"_index":"' + INDEX + '","_type":"' + TYPE + '"}}\n'
    data += json.dumps(doc) + "\n"
    _bulk(ES_HOST, data)
    print("%s : Write last data that length is %s" % (str(datetime.now()), len(data)))

  credentials = _get_credentials()
  url = _create_url(host, "/_template/rds_errorlog?ignore_conflicts=true")
  response = es_request(url, "PUT", credentials, data=json.dumps(d))
  if not response.ok:
    print(response.text)


def _validate_log_date(now, line):
  delta = timedelta(hours=2)

  m = REG_GENERAL_ERR.match(line)
  if m:
    log_time = datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S")
    log_time = log_time.replace(tzinfo=tz.tzutc()).astimezone(zoneinfo.gettz(TIMEZONE))
    log_time = log_time.replace(tzinfo=None)
    if (now - log_time) > delta:
      return False
  elif BEGIN_DEADLOCK in line:
    m = REG_DEADLOCK.match(line)
    log_time = datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S")
    log_time = log_time.replace(tzinfo=tz.tzutc()).astimezone(zoneinfo.gettz(TIMEZONE))
    log_time = log_time.replace(tzinfo=None)
    if (now - log_time) > delta:
      return False

  return True


def _create_index(host):
  d = dict()
  d["template"] = "rds_errorlog-*"
  d["settings"] = dict()
  d["settings"]["number_of_shards"] = 1


def _bulk(host, doc):
  credentials = _get_credentials()
  url = _create_url(host, "/_bulk")
  response = es_request(url, "POST", credentials, data=doc)
  if not response.ok:
    print(response.text)
    print("Request is failed")
    return False

  print("Request is sent successfully")
  return True


def _get_credentials():
  return Credentials(
    os.environ["AWS_ACCESS_KEY_ID"],
    os.environ["AWS_SECRET_ACCESS_KEY"],
    os.environ["AWS_SESSION_TOKEN"])


def _create_url(host, path, ssl=False):
  if not path.startswith("/"):
    path = "/" + path

  if ssl:
    return "https://" + host + path
  else:
    return "http://" + host + path


def getEC2InstancesInVpc(region, vpc):
  ec2list = dict()

  ec2 = boto3.resource("ec2", region_name=region)
  vpc = ec2.Vpc(vpc)
  for i in vpc.instances.all():
    for tag in i.tags:
      if tag['Key'] == 'Name':
        ec2list[i.private_ip_address] = "".join(tag['Value'].split())
  return ec2list


def es_request(url, method, credentials, region=ES_DEFAULT_REGION, headers=None, data=None):
  return request(url, method, credentials, "es", region, headers, data)


def request(url, method, credentials, service_name, region=None, headers=None, data=None):
  if not region:
    region = os.environ["AWS_REGION"]

  aws_request = AWSRequest(url=url, method=method, headers=headers, data=data)
  SigV4Auth(credentials, service_name, region).add_auth(aws_request)
  return PreserveAuthSession().send(aws_request.prepare())


lambda_handler()
