# rdsmant
rdsmant stands for RDS MANagement Tool


##Prerequisites

- Python version 2.7 or greater.
- pip install (boto3, PyYaml, Elasticsearch)

##Getting Started


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

# Report daily RDS status.
# I'm sorry, It's in prepration...

```

##Contact

[stdjangyoungwhan@gmail.com](https://github.com/JangYoungWhan)
