set -e
cd /tmp
rm -rf lit_tmp_dl
mkdir lit_tmp_dl
cd lit_tmp_dl
wget $1
tar zxf *.tar.gz
hadoop fs -rm -r -f -skipTrash $2
hadoop fs -mkdir -p $2
hadoop fs -copyFromLocal *.txt $2