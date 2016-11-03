import boto3
import datetime


class RdsDailyReporter:
    def __init__(self):
        self._client = boto3.client("cloudwatch", region_name = "ap-northeast-2")

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

    def getTargetRdsStatistics(self):
        for m in self._target_metric_map.keys():
            #print("get..." + self._target_metric_map[m])
            result = self.getRdsMetrics("tb-test", m)
            if len(result["Datapoints"]) > 0:
                v = result["Datapoints"][0]["Maximum"]
                u = self._target_metric_map[m]
                if m in self._scale:
                    v = v * self._scale[m][0]
                    u = self._scale[m][1]
                print("[%s]\t: %0.6f %s" % (m, v, u))
            else:
                print("[%s]\t: %0.6f %s" % (m, 0, self._target_metric_map[m]))


if __name__ == "__main__":
    reporter = RdsDailyReporter()
    reporter.getTargetRdsStatistics()

