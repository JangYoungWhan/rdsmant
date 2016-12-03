import boto3
import datetime


rds_endpoint = (
    {
        "id" : "tb-test",
        "region": "ap-northeast-1"
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
                u = self._target_metric_map[m]
                if m in self._scale:
                    v = v * self._scale[m][0]
                    u = self._scale[m][1]
                print("[%s]\t: %0.6f %s (%s)" % (m, v, u, t))
            else:
                print("[%s]\t: %0.6f %s" % (m, 0, self._target_metric_map[m]))


if __name__ == "__main__":
    reporter = RdsDailyReporter()

    for rds in rds_endpoint:
        rds_id = rds["id"]
        region = rds["region"]
        print("Get status of %s in %s." %(rds_id, region))
        reporter.getTargetRdsStatistics(rds_id, region)
        print("==============")
