# Literary Word Count

## Overview

This app does a word count of some of the works of Shakespeare, Tolstoy and Cervantes.

The purpose is a standard benchmark for a small batch job that can be used to verify a cluster is working as expected.

## Requirements

* [Maven](https://maven.apache.org/docs/3.0.5/release-notes.html) 3.0.5 or later
* [Java JDK](https://docs.oracle.com/javase/8/docs/technotes/guides/install/install_overview.html) 1.8 or later

## Build

To build the example applications use:

````
mvn clean package
````

This command should be run at the root of the repository and will build the application binary, and the application package. It runs once as an oozie workflow application.  It will create package files in the app-package-wf/target directories. They will be called literary-word-count-app-wf-{version}.tar.gz.


## Files in the package

- `hdfs.json`: creates the destination folder in HDFS, specifies not to delete it when the application is destroyed.
- `log4j.properties`: defines the log level and behaviour for the application.
- `properties.json`: contains default properties that may be overriden at application creation time.
- `workflow.xml`: Oozie workflow definition that run the spark job. See Oozie documenation for more information.
- `coordinator.xml`: Oozie coordinator definition that sets the schedule for running the job. See Oozie documenation for more information.

## Deploying the package and creating an application

The PNDA console can be used to deploy the application package to a cluster and then to create an application instance. The console is available on port 80 on the edge node.

The application obtains the source data automatically from s3, see the properties.json file for the default location for this data.

To make the package available for deployment it must be uploaded to a package repository. The default implementation is an OpenStack Swift container. The package may be uploaded via the PNDA repository manager which abstracts the container used, or by manually uploading the package to the container.
