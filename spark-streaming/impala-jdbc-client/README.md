## Driver Installation

Impala can be driven using the Hive JDBC driver - the upstream of this can be found at `https://github.com/apache/hive/tree/master/jdbc/src/java/org/apache/hive/jdbc`.

At the time of writing Cloudera distribute an Apache 2.0 licensed version of this driver which is installable using yum on RHEL.

`sudo wget 'https://archive.cloudera.com/cdh5/redhat/7/x86_64/cdh/cloudera-cdh5.repo' \
    -O /etc/yum.repos.d/cloudera
`

`sudo yum clean all
`

`sudo yum install hive-jdbc
`

## Usage

Example command line:

    java -cp target/impala-jdbc-client-1.0.jar:/usr/lib/hive/lib/*:/usr/lib/hadoop/* com.cisco.pnda.examples.impalaclient.Client example_table 1467046648000 1467046649000
