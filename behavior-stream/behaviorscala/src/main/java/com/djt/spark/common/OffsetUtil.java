package com.djt.spark.common;

import kafka.common.TopicAndPartition;
import org.apache.spark.streaming.kafka.KafkaCluster;
import org.apache.spark.streaming.kafka.OffsetRange;
import scala.Predef;
import scala.Tuple2;
import scala.collection.JavaConversions;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

public class OffsetUtil
{
  /*
  * 将offset写入zk
  * */
  public static void offsetToZk(final Map<String, String> kafkaParams,
                                final AtomicReference<OffsetRange[]> offsetRanges,
                                final String groupId) {

    KafkaCluster kafkaCluster = getKafkaCluster(kafkaParams);

    for (OffsetRange o : offsetRanges.get()) {

      // 封装topic.partition 与 offset对应关系 java Map
      TopicAndPartition topicAndPartition = new TopicAndPartition(o.topic(), o.partition());
      Map<TopicAndPartition, Object> topicAndPartitionObjectMap = new HashMap();
      topicAndPartitionObjectMap.put(topicAndPartition, o.untilOffset());

      // 转换java map to scala immutable.map
      scala.collection.mutable.Map<TopicAndPartition, Object> testMap = JavaConversions.mapAsScalaMap(topicAndPartitionObjectMap);

      scala.collection.immutable.Map<TopicAndPartition, Object> scalatopicAndPartitionObjectMap =
              testMap.toMap(new Predef.$less$colon$less<Tuple2<TopicAndPartition, Object>, Tuple2<TopicAndPartition, Object>>() {
                public Tuple2<TopicAndPartition, Object> apply(Tuple2<TopicAndPartition, Object> v1) {
                  return v1;
                }
              });

      // 更新offset到kafkaCluster
      kafkaCluster.setConsumerOffsets(groupId, scalatopicAndPartitionObjectMap);
    }
  }

  /*
  * 获取kafka每个分区消费到的offset,以便继续消费
  * */
  public static Map<TopicAndPartition, Long> getConsumerOffsets(Map<String, String> kafkaParams, String groupId, String topic)
  {
    Set<String> topicSet = new HashSet(Arrays.asList(topic.split(",")));
    KafkaCluster kafkaCluster = getKafkaCluster(kafkaParams);

    scala.collection.mutable.Set<String> mutableTopics = JavaConversions.asScalaSet(topicSet);
    scala.collection.immutable.Set<String> immutableTopics = mutableTopics.toSet();
    scala.collection.immutable.Set<TopicAndPartition> topicAndPartitionSet2 = (scala.collection.immutable.Set<TopicAndPartition>) kafkaCluster.getPartitions(immutableTopics).right().get();

    // kafka direct stream 初始化时使用的offset数据
    Map<TopicAndPartition, Long> consumerOffsetsLong = new HashMap();

    // 没有保存offset时（该group首次消费时）, 各个partition offset 默认为0
    if (kafkaCluster.getConsumerOffsets(groupId, topicAndPartitionSet2).isLeft()) {

      Set<TopicAndPartition> topicAndPartitionSet1 = JavaConversions.setAsJavaSet(topicAndPartitionSet2);

      for (TopicAndPartition topicAndPartition : topicAndPartitionSet1) {
        consumerOffsetsLong.put(topicAndPartition, 0L);
      }
    } else { // offset已存在, 使用保存的offset
      scala.collection.immutable.Map<TopicAndPartition, Object> consumerOffsetsTemp =
        (scala.collection.immutable.Map<TopicAndPartition, Object>) kafkaCluster.getConsumerOffsets(groupId, topicAndPartitionSet2).right().get();

      Map<TopicAndPartition, Object> consumerOffsets = JavaConversions.mapAsJavaMap(consumerOffsetsTemp);

      Set<TopicAndPartition> topicAndPartitionSet1 = JavaConversions.setAsJavaSet(topicAndPartitionSet2);

      for (TopicAndPartition topicAndPartition : topicAndPartitionSet1) {
        Long offset = (Long) consumerOffsets.get(topicAndPartition);
        consumerOffsetsLong.put(topicAndPartition, offset);
      }
    }

    return consumerOffsetsLong;
  }

  public static KafkaCluster getKafkaCluster(Map<String, String> kafkaParams) {
    scala.collection.mutable.Map<String, String> testMap = JavaConversions.mapAsScalaMap(kafkaParams);
    scala.collection.immutable.Map<String, String> scalaKafkaParam =
            testMap.toMap(new Predef.$less$colon$less<Tuple2<String, String>, Tuple2<String, String>>()
            {
              public Tuple2<String, String> apply(Tuple2<String, String> v1)
              {
                return v1;
              }
            });

    return new KafkaCluster(scalaKafkaParam);
  }

  public static void printConfig(Properties serverProps) {
    Iterator<Map.Entry<Object, Object>> it1 = serverProps.entrySet().iterator();
    while (it1.hasNext()) {
      Map.Entry<Object, Object> entry = it1.next();
      System.out.println(entry.getKey().toString()+"="+entry.getValue().toString());
    }
  }
}
