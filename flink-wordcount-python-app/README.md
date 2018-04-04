# Example Flink Wordcount Python Batch Application

## Overview

The example batch application shows an example of an application that can be deployed using the PNDA Deployment Manager. (See the `platform-deployment-manager` project for details.)

The application is a tar file containing binaries and configuration files required to perform batch processing. 

This example application counts the number of occurance of each word present in the input file.

This application runs as a coordinator on a regular schedule.

## Requirements

* [Maven](https://maven.apache.org/docs/3.0.5/release-notes.html) 3.0.5
* [Java JDK](https://docs.oracle.com/javase/8/docs/technotes/guides/install/install_overview.html) 1.8

## Build

To build the example applications use:

````
mvn clean package
````

This command should be run at the root of the repository and will build the application package. It will create a package file flink-wordcount-python-app-{version}.tar.gz in the app-package/target directory. 

## Flink Job

This is a very basic job that reads text from an input file and writes the wordcount to an output file

## Files in the package

- `properties.json`: contains default properties that may be overriden at application creation time. A mandatory property need to be specified by the user i.e. `"job_type": "flink"`, to differentiate from other type of jobs.
- `workflow.xml`: Oozie workflow definition that run the ssh action. See Oozie documenation for more information.
- `coordinator.xml`: Oozie coordinator definition that sets the schedule for running the job. See Oozie documenation for more information.
- `lib/job.py`: Python code that implements the wordcount job.

## Deploying the package and creating an application

The PNDA console can be used to deploy the application package to a cluster and then to create an application instance. The console is available on port 80 on the edge node.

While creating application, specify the input file path and output file path in `application_args` of properties.json.

If the user does not provide any input file, it will automatically process example text present in the code.

To make the package available for deployment it must be uploaded to a package repository. The default implementation is an OpenStack Swift container. The package may be uploaded via the PNDA repository manager which abstracts the container used, or by manually uploading the package to the container.

`Note`: Oozie does not natively support an action for submitting Flink job on yarn-cluster.  PNDA deployment-manager creates a shell script to submit flink job. This shell script is run through Oozie SSH action, to submit the flink application.
