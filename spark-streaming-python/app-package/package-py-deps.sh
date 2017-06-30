rm -rf pydeps-stage
mkdir pydeps-stage; cd pydeps-stage
wget http://archive.apache.org/dist/avro/avro-1.8.1/py/avro-1.8.1.tar.gz
tar zxf avro-1.8.1.tar.gz
cd avro-1.8.1
python setup.py bdist_egg
cp dist/avro-1.8.1-py2.7.egg ../../src/main/resources/sparkStreaming/example