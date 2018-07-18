sudo -u guoyuanpei spark-submit \
--class com.marcpoint.TouchPointNewNew \
--name "Hive on Spark" \
--master yarn-cluster \
--conf spark.yarn.maxAppAttempts=1 \
--executor-memory 9G \
--conf spark.yarn.executor.memoryOverhead=2048 \
--executor-cores 4 \
--driver-memory 10G \
--conf spark.yarn.driver.memoryOverhead=1536 \
--driver-cores 3 \
--conf spark.dynamicAllocation.maxExecutors=64 \
--jars /usr/share/java/mysql-connector-java.jar,/usr/share/java/postgresql-9.0-801.jdbc4.jar \
/home/guoyuanpei/sparkApp/touchPoint/touchPoint.jar "2018-04-14" "1" "172.16.1.100"

nc -lk 9999

yarn application -kill application_1526610357916_0884




------------------------------ das 测试 ---------------------------------

sudo -u guoyuanpei spark-submit \
--class com.marcpoint.DasRegex \
--name "Hive on Spark" \
--master yarn-cluster \
--conf spark.yarn.maxAppAttempts=1 \
--executor-memory 9G \
--conf spark.yarn.executor.memoryOverhead=2048 \
--executor-cores 4 \
--driver-memory 10G \
--conf spark.yarn.driver.memoryOverhead=1536 \
--driver-cores 3 \
--driver-java-options "-Xss10m" \
--conf spark.dynamicAllocation.maxExecutors=64 \
--jars /usr/share/java/mysql-connector-java.jar,/usr/share/java/postgresql-9.0-801.jdbc4.jar \
/home/guoyuanpei/sparkApp/touchPoint/touchPoint.jar "kcc_diapers_brand_sentiment0612.xlsx" "transforms.test_new_lexicon" "transforms.test_new_lexicon_result" "overwrite" "255" "1 = 1"




sh /Users/guoyuanpei/Downloads/deplow_touch.sh