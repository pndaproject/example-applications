## Driver Installation

Impala can be driven using the Hive JDBC driver - the upstream of this can be found at `https://github.com/apache/hive/tree/master/jdbc/src/java/org/apache/hive/jdbc`.

At the time of writing Cloudera distribute an Apache 2.0 licensed version of this driver which is installable using apt-get on Ubuntu Trusty.

`sudo wget 'https://archive.cloudera.com/cdh5/ubuntu/trusty/amd64/cdh/cloudera.list' \
    -O /etc/apt/sources.list.d/cloudera.list
`

`sudo apt-get update
`

`sudo apt-get install hive-jdbc
`

## Usage

Example command line:

    java -cp target/impala-jdbc-client-1.0.jar:/usr/lib/hive/lib/*:/usr/lib/hadoop/* com.cisco.pnda.examples.impalaclient.Client example_table 1467046648000 1467046649000
