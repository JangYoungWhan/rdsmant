# rdsmant
rdsmant stands for RDS MANagement Tool


## Prerequisites

- Python version 2.7 or greater.
- pip install (boto3, PyYaml, Elasticsearch)

## Getting Started


```bash
# Install python modules.
sudo pip install pyyaml
sudo pip install elasticsearch
```

```bash
# Register crontab for every 10 minutes due to the creation time of RDS log.
10 * * * * python2.7 errorlog2es.py
10 * * * * python2.7 slowquery2es.py

# Run on background or using by screen
python2.7 rdschker.py

# Report daily RDS status. (In fact, I have been developed this script on python 3.5 version.)
# '-b' indicates begin time in UTC. '-e' indicates end time in UTC.
python rds-daily-reporter.py -b 20161211 -e 20161212

# You can see like this. (The value and UTC time mean peak in that time range)
Get status of tb-test in ap-northeast-2.
[CPUUtilization]	: 1.360000 Percent (2016-12-11 00:06:00+00:00)
[WriteThroughput]	: 0.009896 MB/s (2016-12-10 16:43:00+00:00)
[DiskQueueDepth]	: 0.002267 Count (2016-12-11 10:15:00+00:00)
[DatabaseConnections]	: 2.000000 Count (2016-12-10 17:47:00+00:00)
[ReadLatency]	: 4.000000 Milliseconds (2016-12-10 15:00:00+00:00)
[WriteLatency]	: 7.428571 Milliseconds (2016-12-11 14:56:00+00:00)
[WriteIOPS]	: 0.833333 Count/Second (2016-12-10 18:20:00+00:00)
[ReadThroughput]	: 0.000716 MB/s (2016-12-10 16:00:00+00:00)
[BinLogDiskUsage]	: 0.000000 MB (2016-12-10 22:29:00+00:00)
[ReadIOPS]	: 1.450145 Count/Second (2016-12-10 16:00:00+00:00)
[ReplicaLag]	: 0.000000 Seconds

```

## Contact

[stdjangyoungwhan@gmail.com](https://github.com/JangYoungWhan)
