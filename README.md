zookeeper
=========
There are two folder in this repository:
zookeeper-server is the base environment contains three zookeeper servers.
zookeeper-work is the main demo project.

## Getting Started
### Start zookeeper servers
cd /zookeeper-server/z1
sudo sh bin/zkServer.sh start

cd /zookeeper-server/z2
sudo sh bin/zkServer.sh start

cd /zookeeper-server/z3
sudo sh bin/zkServer.sh start


### Run demo programme
**The main programe's path:**  zookeeper-work / src / main / java / com / gg / work

**Lanuch N feature jobs:**
run "RegisterJob.java" N times

**Client sends request for feature extraction:**
run "ExtractFeatureClient.java"


### Code coverage report
zookeeper-work/zookeeper_report.png
