# h2o Launcher

## Overview

This project runs the h2o data science platform as an application on PNDA.

*** Notes ***

The sparkStreaming deployment component is being re-used to launch this application as a PoC.

This only works for PNDA running on Ubuntu (not Redhat).


## Requirements

* [Maven](https://maven.apache.org/docs/3.0.5/release-notes.html) 3.0.5

## h2o Distribution

[Download](http://www.h2o.ai/download/) a version of h2o that is compatible with the target PNDA version then extract h2odriver.jar and place it in

```
app-package/src/main/resources/sparkStreaming/h2o
```

For PNDA 3.2 use

```
http://h2o-release.s3.amazonaws.com/h2o/rel-turing/10/h2o-3.10.0.10-cdh5.8.zip
```

## Build

To build the h2o application use:

```
mvn clean package
```

This command should be run at the root of the repository and will build the application package to

```
app-package/target/h2o-launcher-{version}.tar.gz
```

## Files in the package

- `application.properties`: unused but required for compatability with the deployment manager.
- `log4j.properties`: unused but required for compatability with the deployment manager.
- `properties.json`: contains default properties that may be overriden at application creation time.
- `upstart.conf`: runs h2o using the hadoop jar command.
- `yarn-kill.py`: kills the yarn job associated with this application.

## Deploying the package and creating an application

The PNDA console can be used to deploy the application package to a cluster and then to create an application instance. The console is available on port 80 on the edge node.

To make the package available for deployment it must be uploaded to a package repository. The default implementation is an OpenStack Swift container. The package may be uploaded via the PNDA repository manager which abstracts the container used, or by manually uploading the package to the container.

## How to access h2o after running the app

After creating and starting the PNDA application, use the PNDA log server to search for the phrase "Open H2O Flow in your web browser". There should be a message with a link like this `Open H2O Flow in your web browser: http://10.0.1.102:54321 `

You may also find it convenient to tail the h2o submit command directly e.g.

```
ubuntu@pnda-cdh-edge:~$ sudo tail -f -n 200 /var/log/upstart/platform_app-h2o-h2o.log
16/10/20 10:17:11 INFO client.RMProxy: Connecting to ResourceManager at pndachu-cdh-mgr-1/10.0.1.228:8032
16/10/20 10:17:12 INFO mapreduce.JobSubmitter: number of splits:1
16/10/20 10:17:12 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1476952813387_0004
16/10/20 10:17:13 INFO impl.YarnClientImpl: Submitted application application_1476952813387_0004
16/10/20 10:17:13 INFO mapreduce.Job: The url to track the job: http://pndachu-cdh-mgr-1:8088/proxy/application_1476952813387_0004/
Job name 'H2O_60513' submitted
JobTracker job ID is 'job_1476952813387_0004'
For YARN users, logs command is 'yarn logs -applicationId application_1476952813387_0004'
Waiting for H2O cluster to come up...
H2O node 10.0.1.102:54321 requested flatfile
Sending flatfiles to nodes...
    [Sending flatfile to node 10.0.1.102:54321]
H2O node 10.0.1.102:54321 reports H2O cluster size 1
H2O cluster (1 nodes) is up
(Note: Use the -disown option to exit the driver after cluster formation)

Open H2O Flow in your web browser: http://10.0.1.102:54321

(Press Ctrl-C to kill the cluster)
Blocking until the H2O cluster shuts down...
```
