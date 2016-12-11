#-*- coding: utf-8 -*-

# Project   : Report RDS status based on cloudwatch metrics with time range.
# Author    : YW. Jang
# Date      : 2016.12.11
#
# Copyright 2016, YW. Jang, All rights reserved.

import argparse
import sys

import boto3
import datetime


rds_endpoint = (
    {
        "id" : "tb-test",
        "region": "ap-northeast-2"
    },
    {
        "id": "tb-test2",
        "region": "ap-northeast-2"
    }
)


class RdsDailyReporter:
    def __init__(self):
        # There is a reference in below.
        # http://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/rds-metricscollected.html
        self._target_metric_map = {
            "CPUUtilization": "Percent",
            "DatabaseConnections": "Count",
            "ReadIOPS": "Count/Second",
            "WriteIOPS": "Count/Second",
            "ReadLatency": "Seconds",
            "WriteLatency": "Seconds",
            "DiskQueueDepth": "Count",
            "ReplicaLag": "Seconds",
            "BinLogDiskUsage": "Bytes",
            "ReadThroughput": "Bytes/Second",
            "WriteThroughput": "Bytes/Second",
        }

        self._scale = {
            # MetricName: [Scaling ratio, Human Readable Unit]
            "ReadLatency": [1000, "Milliseconds"],
            "WriteLatency": [1000, "Milliseconds"],
            "BinLogDiskUsage": [1/(1024*1024), "MB"],
            "ReadThroughput": [1/(1024*1024), "MB/s"],
            "WriteThroughput": [1/(1024*1024), "MB/s"],
        }

        self._from = datetime.datetime.utcnow() - datetime.timedelta(days=1)
        self._to = datetime.datetime.utcnow()

    def setDateRange(self, begin, end):
        self._from = self.buildDateTime(int(begin[0:4]), int(begin[4:6]), int(begin[6:8]))
        self._to = self.buildDateTime(int(end[0:4]), int(end[4:6]), int(end[6:8]))

    def buildDateTime(self, year, month, day, h=0, m=0, s=0):
        return datetime.datetime(year=year, month=month, day=day,
                                 hour=h, minute=m, second=s)

    def getRdsMetrics(self, rds_id, metric):
        response = self._client.get_metric_statistics(
            Namespace = "AWS/RDS",
            MetricName = metric,
            Dimensions = [
                {
                    "Name": "DBInstanceIdentifier",
                    "Value": rds_id
                }
            ],
            StartTime = datetime.datetime.utcnow() - datetime.timedelta(days=1),
            EndTime = datetime.datetime.utcnow(),
            Period = 60,
            Statistics = ["Maximum"],
            Unit = self._target_metric_map[metric]
        )
        return response

    def getTargetRdsStatistics(self, rds_id, region):
        self._client = boto3.client("cloudwatch", region_name=region)
        for m in self._target_metric_map.keys():
            result = self.getRdsMetrics(rds_id, m)
            if len(result["Datapoints"]) > 0:
                datapoint = max(result["Datapoints"], key=lambda x: x["Maximum"])
                v = datapoint["Maximum"]
                t = datapoint["Timestamp"]
                #v = result["Datapoints"][0]["Maximum"]
                u = self._target_metric_map[m]
                if m in self._scale:
                    v = v * self._scale[m][0]
                    u = self._scale[m][1]
                print("[%s]\t: %0.6f %s (%s)" % (m, v, u, t))
            else:
                print("[%s]\t: %0.6f %s" % (m, 0, self._target_metric_map[m]))


if __name__ == "__main__":
    reporter = RdsDailyReporter()

    parser = argparse.ArgumentParser()
    parser.add_argument("-b", "--begin ", dest="begin", help="from", type=str, required=True)
    parser.add_argument("-e", "--end ", dest="end", help="to", type=str, required=True)

    try:
        args = parser.parse_args()
        reporter.setDateRange(args.begin, args.end)
    except Exception as e:
        parser.print_help()
        print(e)
        sys.exit(0)

    for rds in rds_endpoint[0:-1]:
        rds_id = rds["id"]
        region = rds["region"]
        print("Get status of %s in %s." %(rds_id, region))
        reporter.getTargetRdsStatistics(rds_id, region)
        print("==============")
