#-*- coding: utf-8 -*-

# Project   : Transfer RDS slowquery log to elastic search.
# Author    : YW. Jang
# Date      : 2016.05.03
#
# Copyright 2016, YW. Jang, All rights reserved.

import boto3
import re
import os
import json
import subprocess
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
INDEX_PREFIX = "rds_slowquerylog"

# Elasticsearch type name is rds instance id.
RDS_ID = "tb-master"

# Enabled to change timezone. If you set UTC, this parameter is blank.
TIMEZONE = "Asia/Seoul"

# Query time format regex.
TIME_REGEX = "^[a-zA-Z#:_ ]+([0-9.]+)[a-zA-Z:_ ]+([0-9.]+)[a-zA-Z:_ ]+([0-9.]+).[a-zA-Z:_ ]+([0-9.]+)$"

# Exclude noise string
NOISE = [
  "/rdsdbbin/mysql/bin/mysqld, Version: 5.6.21-log (MySQL Community Server (GPL)). started with:",
  "Tcp port: 3306  Unix socket: /tmp/mysql.sock",
  "Time                 Id Command    Argument"
]

R = re.compile(TIME_REGEX)
NOW = datetime.now()
INDEX = INDEX_PREFIX + "-" + datetime.strftime(NOW, "%Y.%m")
TYPE = RDS_ID
SLOWQUERYLOG_PREFIX = "slowquery/mysql-slowquery.log."

# RDS region which you want to crawling error log.
AWS_RDS_REGION_ID = "us-west-1"

# If you have ec2 instances, then It need region and VPC involving instances.
AWS_EC2_REGION_ID = "us-west-1"
AWS_EC2_VPC_ID = "vpc-xxxxx"


def lambda_handler():#(event, context):
  print("%s : Run slowquery2es.py" % (str(datetime.now())))
  client = boto3.client("rds", region_name=AWS_RDS_REGION_ID)
  db_files = client.describe_db_log_files(DBInstanceIdentifier=RDS_ID)

  log_filename = SLOWQUERYLOG_PREFIX + str(datetime.utcnow().hour)
  if not filter(lambda log: log["LogFileName"] == log_filename, db_files["DescribeDBLogFiles"]):
    print("%s does not exist!" % (log_filename))
    return
  
  body = client.download_db_log_file_portion(
    DBInstanceIdentifier=RDS_ID,
    LogFileName=log_filename
  )["LogFileData"]
  
  data = ""
  doc = {}

  lines = body.split("\n")
  if len(lines) > 0:
    if not _validate_log_date(NOW, lines):
      print("%s already read log before!" % (log_filename))
      return
  else:
    print("%s is empty!" % (log_filename))
    return
    
  # Get ready for extracting log file.
  ec2list = getEC2InstancesInVpc(AWS_EC2_REGION_ID, AWS_EC2_VPC_ID)
  _create_index(ES_HOST)
  print("%s : Write %s in %s" % (str(datetime.now()), log_filename, INDEX))
  
  for line in body.split("\n"):
    if not line or line in NOISE:
      continue
    elif line.startswith("# Time: "):
      if doc:
        data += '{"index":{"_index":"' + INDEX + '","_type":"' + TYPE + '"}}\n'
        doc["fingerprint"] = _clean_fingerprint(doc["fingerprint"])
        data += json.dumps(doc) + "\n"
      if len(data) > 100000:
        data = _remove_dup_lf(data)
        _bulk(ES_HOST, data)
        print("%s : Write data that length is %s" % (str(datetime.now()), len(data)))
        data = ""

      timestamp = datetime.strptime(line[8:], "%y%m%d %H:%M:%S")
      if TIMEZONE:
        timestamp = timestamp.replace(tzinfo=tz.tzutc()).astimezone(zoneinfo.gettz(TIMEZONE))
      doc = {"timestamp": timestamp.isoformat()}
    elif line.startswith("# User@Host: "):
      doc["user"] = line.split("[")[1].split("]")[0]
      doc["client"] = line.split("[")[2].split("]")[0]
      doc["client_id"] = line.split(" Id: ")[1]
      ip_addr = doc["client"]
      if ip_addr not in ec2list:
        doc["name"] = "Missed"
      else:
        doc["name"] = ec2list[ip_addr]
    elif line.startswith("# Query_time: "):
      match = R.match(line).groups(0)
      doc["query_time"] = match[0]
      doc["lock_time"] = match[1]
      doc["rows_sent"] = match[2]
      doc["rows_examined"] = match[3]
    else:
      if doc.get("sql"):
        doc["sql"] += "\n" + line
        doc["fingerprint"] += "\n" + _get_fingerprint(line)
      else:
        doc["sql"] = line
        doc["fingerprint"] = _get_fingerprint(line)

  if doc:
    data += '{"index":{"_index":"' + INDEX + '","_type":"' + TYPE + '"}}\n'
    doc["fingerprint"] = _remove_dup_lf(doc["fingerprint"])
    data += json.dumps(doc) + "\n"
    _bulk(ES_HOST, data)
    print("%s : Write last data that length is %s" % (str(datetime.now()), len(data)))


def _validate_log_date(now, lines):
  delta = timedelta(hours=2)
  
  for line in lines:
    if not line or line in NOISE:
      continue
    elif line.startswith("# Time: "):
      log_time = datetime.strptime(line[8:], "%y%m%d %H:%M:%S")
    if (now - log_time) < delta:
      return False
    else:
      return True
      
  return True


def _create_index(host):
  d = dict()
  d["template"] = "rds_slowquerylog-*"
  d["settings"] = dict()
  d["settings"]["number_of_shards"] = 1
  d["mappings"] = dict()
  d["mappings"][RDS_ID] = dict()
  d["mappings"][RDS_ID]["properties"] = dict()
  d["mappings"][RDS_ID]["properties"]["query_time"] = {"type": "float", "index": "not_analyzed"}
  d["mappings"][RDS_ID]["properties"]["lock_time"] = {"type": "float", "index": "not_analyzed"}
  d["mappings"][RDS_ID]["properties"]["rows_sent"] = {"type": "integer", "index": "not_analyzed"}
  d["mappings"][RDS_ID]["properties"]["rows_examined"] = {"type": "integer", "index": "not_analyzed"}

  credentials = _get_credentials()
  url = _create_url(host, "/_template/rds_slowquerylog?ignore_conflicts=true")
  response = es_request(url, "PUT", credentials, data=json.dumps(d))
  if not response.ok:
    print(response.text)
  
  
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
  
  
def _skip_fingerprint(s):
  SET_TIMESTAMP = "set timestamp="
  USE_DATABASE = "use "

  if s.startswith(SET_TIMESTAMP):
    return ""
  elif s.startswith(USE_DATABASE):
    return ""
  else:
    # Substitue multiple line feed to single line feed.
    s = re.sub(r"(\n)+", r"\n", s)
    return s


def _remove_dup_lf(s):
  stripped = s.strip()
  # Substitue multiple line feed to single line feed.
  stripped = re.sub(r"(\n)+", r"\n", stripped)
  return stripped
  
  
def _get_fingerprint(sql):
  cmd = "pt-fingerprint --query '%s'" % sql

  try:
    return _skip_fingerprint(subprocess.check_output(cmd, shell=True))
  except:
    return sql


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


def request(url, method, credentials, service_name, region=None, headers=None, data=None):
  if not region:
    region = os.environ["AWS_REGION"]

  aws_request = AWSRequest(url=url, method=method, headers=headers, data=data)
  SigV4Auth(credentials, service_name, region).add_auth(aws_request)
  return PreserveAuthSession().send(aws_request.prepare())


def es_request(url, method, credentials, region=ES_DEFAULT_REGION, headers=None, data=None):
  return request(url, method, credentials, "es", region, headers, data)

lambda_handler()