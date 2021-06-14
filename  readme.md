项目:
https://github.com/BigDataScholar/FlinkECUserBehaviorAnalysis/tree/master/src/main/java/com/hypers

kafka-console-consumer.sh --bootstrap-server 10.103.17.101:9092,10.103.17.102:9092,10.103.17.103:9092 --topic test_pi --group flink
kafka-consumer-groups.sh --bootstrap-server 10.103.17.101:9092,10.103.17.102:9092,10.103.17.103:9092 --reset-offsets --to-latest  --topic test_pi --group flink --execute