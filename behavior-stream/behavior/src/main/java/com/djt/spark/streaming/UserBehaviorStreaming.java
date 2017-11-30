package com.djt.spark.streaming;

import com.djt.model.SingleUserBehaviorRequestModel;
import com.djt.model.UserBehaviorStatModel;
import com.djt.model.UserBehavorRequestModel;
import com.djt.spark.key.UserHourPackageKey;
import com.djt.spark.service.BehaviorStatService;
import com.djt.spark.monitor.MonitorStopThread;
import com.djt.utils.DateUtils;
import com.djt.utils.JSONUtil;
import com.djt.utils.MyStringUtil;
import com.djt.utils.PropertiesUtil;
import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaCluster;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;
import scala.Predef;
import scala.Tuple2;
import scala.collection.JavaConversions;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

public class UserBehaviorStreaming
{
  public static Logger log = Logger.getLogger(UserBehaviorStreaming.class);

  //判断是否出入了配置文件，如果没有传入则退出
  public static void main(String[] args) throws Exception
  {
    if (args.length < 1) {
      System.err.println("Usage: UserBehaviorStreaming ConfigFile");
      System.exit(1);
    }

    //获取配置文件路径
    String configFile = args[0];
    final Properties serverProps = PropertiesUtil.getProperties(configFile);
    //获取checkpoint的hdfs路径
    String checkpointPath = serverProps.getProperty("streaming.checkpoint.path");
    printConfig(serverProps);

    //如果checkpointPath hdfs目录下的有文件，则反序列化文件生产context,否则使用函数createContext返回的context对象
    JavaStreamingContext javaStreamingContext = JavaStreamingContext.getOrCreate(checkpointPath, createContext(serverProps));
    javaStreamingContext.start();

    //每隔20秒钟监控是否有停止指令,如果有则优雅退出streaming
    Thread thread = new Thread(new MonitorStopThread(javaStreamingContext,serverProps));
    thread.start();

    javaStreamingContext.awaitTermination();
  }

  /**
   * 根据配置文件以及业务逻辑创建JavaStreamingContext
   * @param serverProps
   * @return
   */
  public static Function0<JavaStreamingContext> createContext(final Properties serverProps) {
    Function0<JavaStreamingContext> createContextFunc = new Function0<JavaStreamingContext>()
    {
      @Override
      public JavaStreamingContext call() throws Exception
      {
        //获取配置中的topic
        String topicStr = serverProps.getProperty("kafka.topic");
        Set<String> topicSet = new HashSet(Arrays.asList(topicStr.split(",")));

        final String groupId = serverProps.getProperty("kafka.groupId");
        //获取批次的时间间隔，比如5s
        final Long streamingInterval = Long.parseLong(serverProps.getProperty("streaming.interval"));
        //获取checkpoint的hdfs路径
        final String checkpointPath = serverProps.getProperty("streaming.checkpoint.path");
        //获取kafka broker列表
        final String metadataBrokerList = serverProps.getProperty("kafka.metadata.broker.list");

        //组合kafka参数
        final Map<String, String> kafkaParams = new HashMap();
        kafkaParams.put("metadata.broker.list", metadataBrokerList);
        //kafkaParams.put("auto.offset.reset","largest");
        kafkaParams.put("group.id",groupId);

        //从zookeeper中获取每个分区的消费到的offset位置
        final KafkaCluster kafkaCluster = getKafkaCluster(kafkaParams);
        Map<TopicAndPartition, Long> consumerOffsetsLong = getConsumerOffsets(kafkaCluster,groupId,topicSet);
        printZkOffset(consumerOffsetsLong);

        //创建sparkconf
        SparkConf sparkConf = new SparkConf().setAppName("UserBehaviorStreaming");
        sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true");
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        sparkConf.set("spark.kryo.registrator", "com.djt.spark.registrator.MyKryoRegistrator");
        sparkConf.set("spark.streaming.kafka.maxRatePerPartition","100");

        //streamingInterval指每隔多长时间执行一个批次
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(streamingInterval));
        javaStreamingContext.checkpoint(checkpointPath);

        //创建kafka DStream
        JavaInputDStream<String> kafkaMessageDStream = KafkaUtils.createDirectStream(
          javaStreamingContext,
          String.class,
          String.class,
          StringDecoder.class,
          StringDecoder.class,
          String.class,
          kafkaParams,
          consumerOffsetsLong,
          new Function<MessageAndMetadata<String, String>, String>() {
            public String call(MessageAndMetadata<String, String> v1) throws Exception {
              return v1.message();
            }
          }
        );

        //需要把每个批次的offset保存到此变量
        final AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference();

        JavaDStream<String> kafkaMessageDStreamTransform = kafkaMessageDStream.transform(new Function<JavaRDD<String>, JavaRDD<String>>() {
          public JavaRDD<String> call(JavaRDD<String> rdd) throws Exception {
            OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
            offsetRanges.set(offsets);

            for (OffsetRange o : offsetRanges.get()) {
              log.info("rddoffsetRange:========================================================================");
              log.info("rddoffsetRange:topic="+o.topic()
                      + ",partition=" + o.partition()
                      + ",fromOffset=" + o.fromOffset()
                      + ",untilOffset=" + o.untilOffset()
                      + ",rddpartitions=" + rdd.getNumPartitions()
                      + ",isempty=" + rdd.isEmpty()
                      + ",id=" + rdd.id());
            }

            return rdd;
          }
        });

        //将kafka中的消息转换成对象并过滤不合法的消息
        JavaDStream<String> kafkaMessageMessageDStreamFilter = kafkaMessageDStreamTransform.filter(new Function<String, Boolean>() {
          @Override
          public Boolean call(String message) throws Exception {
            try {
              UserBehavorRequestModel requestModel = JSONUtil.json2Object(message, UserBehavorRequestModel.class);

              if (requestModel == null ||
                      requestModel.getUserId() == 0 ||
                      requestModel.getSingleUserBehaviorRequestModelList() == null ||
                      requestModel.getSingleUserBehaviorRequestModelList().size() == 0) {
                return false;
              }

              return true;
            } catch (Exception e) {
              log.error("json to UserBehavorRequestModel error", e);
              return false;
            }
          }
        });

        //将每条用户行为转换成键值对，建是我们自定义的key,值是使用应用的时长，并统计时长
        JavaPairDStream<UserHourPackageKey,Long> javaPairDStream = kafkaMessageMessageDStreamFilter.flatMapToPair(new PairFlatMapFunction<String, UserHourPackageKey, Long>() {
          @Override
          public Iterator<Tuple2<UserHourPackageKey, Long>> call(String message) throws Exception {
            List<Tuple2<UserHourPackageKey, Long>> list = new ArrayList();

            UserBehavorRequestModel requestModel;
            try {
              requestModel = JSONUtil.json2Object(message, UserBehavorRequestModel.class);
            } catch (Exception e) {
              log.error("event body is Invalid,message=" + message, e);
              return list.iterator();
            }

            if (requestModel == null) {
              return list.iterator();
            }

            List<SingleUserBehaviorRequestModel> singleList = requestModel.getSingleUserBehaviorRequestModelList();

            for (SingleUserBehaviorRequestModel singleModel : singleList) {
              UserHourPackageKey key = new UserHourPackageKey();
              key.setUserId(requestModel.getUserId());
              key.setHour(DateUtils.getDateStringByMillisecond(DateUtils.HOUR_FORMAT, requestModel.getBeginTime()));
              key.setPackageName(singleModel.getPackageName());

              Tuple2<UserHourPackageKey, Long> t = new Tuple2(key, singleModel.getActiveTime() / 1000);
              list.add(t);
            }

            return list.iterator();
          }
        }).reduceByKey(new Function2<Long, Long, Long>() {
          @Override
          public Long call(Long long1, Long long2) throws Exception {
            return long1 + long2;
          }
        });

        //将每个用户的统计时长写入hbase
        javaPairDStream.foreachRDD(new VoidFunction<JavaPairRDD<UserHourPackageKey, Long>>()
        {
          @Override
          public void call(JavaPairRDD<UserHourPackageKey, Long> rdd) throws Exception
          {
            rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<UserHourPackageKey, Long>>>()
            {
              @Override
              public void call(Iterator<Tuple2<UserHourPackageKey, Long>> it) throws Exception
              {
                BehaviorStatService service = BehaviorStatService.getInstance(serverProps);

                while (it.hasNext()) {
                  Tuple2<UserHourPackageKey, Long> t = it.next();
                  UserHourPackageKey key = t._1();

                  UserBehaviorStatModel model = new UserBehaviorStatModel();
                  model.setUserId(MyStringUtil.getFixedLengthStr(key.getUserId() + "", 10));
                  model.setHour(key.getHour());
                  model.setPackageName(key.getPackageName());
                  model.setTimeLen(t._2());

                  service.addTimeLen(model);
                }
              }
            });

            //kafka offset写入zk
            offsetToZk(kafkaCluster, offsetRanges, groupId);
          }
        });

        return javaStreamingContext;
      }
    };

    return createContextFunc;
  }

  /*
  * 将offset写入zk
  * */
  public static void offsetToZk(final KafkaCluster kafkaCluster,
                                final AtomicReference<OffsetRange[]> offsetRanges,
                                final String groupId) {
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
  public static Map<TopicAndPartition, Long> getConsumerOffsets(KafkaCluster kafkaCluster, String groupId, Set<String> topicSet)
  {
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

  /**
   * 打印配置文件
   * @param serverProps
   */
  public static void printConfig(Properties serverProps) {
    Iterator<Map.Entry<Object, Object>> it1 = serverProps.entrySet().iterator();
    while (it1.hasNext()) {
      Map.Entry<Object, Object> entry = it1.next();
      log.info(entry.getKey().toString()+"="+entry.getValue().toString());
    }
  }

  /**
   * 打印从zookeeper中获取的每个分区消费到的位置
   * @param consumerOffsetsLong
   */
  public static void printZkOffset(Map<TopicAndPartition, Long> consumerOffsetsLong) {
    Iterator<Map.Entry<TopicAndPartition, Long>> it1 = consumerOffsetsLong.entrySet().iterator();
    while (it1.hasNext()) {
      Map.Entry<TopicAndPartition, Long> entry = it1.next();
      TopicAndPartition key = entry.getKey();
      log.info("zookeeper offset:partition=" + key.partition()+",beginOffset=" + entry.getValue());
    }
  }
}
