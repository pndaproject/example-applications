# Example Streaming Application (Spark 2 Structured Streaming API)

## Overview

The example streaming application shows an example of an application that can be deployed using the PNDA Deployment Manager. (See the `platform-deployment-manager` project for details.)

This example uses the Spark2 Structured Streaming APIs. When PNDA is configured to use HDP both Spark1 and Spark2 are deployed side by side. On CDH only Spark1 is deployed, so Spark2 must be installed as a replacement for Spark1 before this app can be run on a CDH PNDA.

The application is a tar file containing binaries and configuration files required to perform some stream processing.

This example application reads events from a network socket and performs basic counting analytics. To run the data source run the command `nc -lk 10072` on the pnda edge node and then type some input into it.

The results are printed into the console output of the spark driver process. To view these, navigate to the log file via the Yarn Resource Manager UI or use the PNDA log server.

Since this application uses the Spark2 API, the special property `spark_version=2` is set in the properties.json file.

## Requirements

* [Maven](https://maven.apache.org/docs/3.0.5/release-notes.html) 3.0.5

## Build

To build the example applications use:

````
mvn clean package
````

This command should be run at the root of the repository and will build the application package. It will create a package file `spark2-streaming-example-app-python-{version}.tar.gz` in the `app-package/target` directory.

## Files in the package

- `application.properties`: config file used by the Spark Streaming scala application.
- `log4j.properties`: defines the log level and behaviour for the spark streaming framework (not the python code).
- `properties.json`: contains default properties that may be overriden at application creation time.
- `job.py`: implements the stream processing in python.

## Deploying the package and creating an application

The PNDA console can be used to deploy the application package to a cluster and then to create an application instance. The console is available on port 80 on the edge node.

To make the package available for deployment it must be uploaded to a package repository. The default implementation is an OpenStack Swift container. The package may be uploaded via the PNDA repository manager which abstracts the container used, or by manually uploading the package to the container.

Make sure to set `input_data_host` to the host that is running the `nc -lk 10072` command that is used to provide data to the application.

Once the application is running, type some commands into the nc input and view the results in the spark driver log file. To view this file navigate to the log via the Yarn Resource Manager UI or use the PNDA log server.




