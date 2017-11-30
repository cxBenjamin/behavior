package com.djt.spark.streaming

import java.util.Properties
import java.util.concurrent.atomic.AtomicReference
import com.djt.model.{SingleUserBehaviorRequestModel, UserBehaviorStatModel, UserBehavorRequestModel}
import com.djt.spark.common.OffsetUtil
import com.djt.spark.key.UserHourPackageKey
import com.djt.spark.monitor.MonitorStopThread
import com.djt.spark.service.BehaviorStatService
import com.djt.utils.{DateUtils, JSONUtil, MyStringUtil, PropertiesUtil}
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, OffsetRange, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}

import scala.collection.JavaConversions

object UserBehaviorStreaming {

  def main(args: Array[String]) {

    //获取配置文件路径
    val configFile: String = args(0)
    val serverProps: Properties = PropertiesUtil.getProperties(configFile)

    OffsetUtil.printConfig(serverProps)

    //获取checkpoint的hdfs路径
    val checkpointPath: String = serverProps.getProperty("streaming.checkpoint.path")

    val sc = StreamingContext.getOrCreate(checkpointPath, () => {createContext(serverProps)})

    sc.start

    //每隔20秒钟监控是否有停止指令,如果有则优雅退出streaming
    val thread: Thread = new Thread(new MonitorStopThread(sc, serverProps))
    thread.start

    sc.awaitTermination
  }

  def createContext(serverProps: Properties): StreamingContext = {
    val sparkConf = new SparkConf().setAppName("UserBehavior")

    val streamingInterval : String = serverProps.getProperty("streaming.interval")

    val t: Long = streamingInterval.toLong
    val sc = new StreamingContext(sparkConf, Seconds(t))

    val topic: String = serverProps.getProperty("kafka.topic")

    val metadataBrokerList: String = serverProps.getProperty("kafka.metadata.broker.list")
    val groupId: String = serverProps.getProperty("kafka.groupId")

    val kafkaParams = Map[String, String]("metadata.broker.list" -> metadataBrokerList, "groupId" -> groupId)

    val javaKafkaParams : java.util.Map[String, String] = JavaConversions.mapAsJavaMap(kafkaParams)
    val javaOffsetsMap : java.util.Map[TopicAndPartition, java.lang.Long] = OffsetUtil.getConsumerOffsets(javaKafkaParams, groupId, topic)
    val javaOffsetsMap1 : java.util.Map[TopicAndPartition, Long] = javaOffsetsMap.asInstanceOf[java.util.Map[TopicAndPartition, Long]]
    val offsets : scala.collection.immutable.Map[TopicAndPartition, Long] = JavaConversions.mapAsScalaMap(javaOffsetsMap1).toMap

    //val offsets: scala.collection.immutable.Map[TopicAndPartition, Long] =
    //  scala.collection.JavaConversions.mapAsScalaMap(OffsetUtil.getConsumerOffsets(javaKafkaParams, groupId, topic)).toMap

    val kafkaMessages: InputDStream[(String,String)] =
      KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder,(String,String)](
        sc,
        kafkaParams,
        offsets,
        (mmd: MessageAndMetadata[String, String]) => (mmd.key(),mmd.message())
      )

    var offsetRanges = Array[OffsetRange]()

    val messages: DStream[(String, String)] = kafkaMessages.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }

    val filterDStream: DStream[(String, String)] = messages.filter { case (key, value) =>
      val requestModel: UserBehavorRequestModel = JSONUtil.json2Object(value, classOf[UserBehavorRequestModel])
      if (requestModel == null ||
          requestModel.getUserId == 0 ||
          requestModel.getSingleUserBehaviorRequestModelList == null
        || requestModel.getSingleUserBehaviorRequestModelList.size == 0) {
        false
      }
      true
    }

    val mapDStream: DStream[UserBehavorRequestModel] = filterDStream.map { case (key, value) =>
      val requestModel: UserBehavorRequestModel = JSONUtil.json2Object(value, classOf[UserBehavorRequestModel])
      requestModel
    }

    val flatMapDStream: DStream[UserBehaviorStatModel] = mapDStream.flatMap[UserBehaviorStatModel] { x: UserBehavorRequestModel =>
      val list = x.getSingleUserBehaviorRequestModelList

      var res: List[UserBehaviorStatModel] = List()

      val it = list.iterator

      while (it.hasNext) {
        val model: SingleUserBehaviorRequestModel = it.next()
        val m: UserBehaviorStatModel = new UserBehaviorStatModel
        m.setUserId(x.getUserId + "")
        m.setHour(DateUtils.getDateStringByMillisecond(DateUtils.HOUR_FORMAT, x.getBeginTime))
        m.setPackageName(model.getPackageName)
        m.setTimeLen(model.getActiveTime)

        res = res :+ m
      }

      res.iterator
    }

    val pairDStream: DStream[(UserHourPackageKey, Long)] = flatMapDStream.map { model: UserBehaviorStatModel =>
      val key: UserHourPackageKey = new UserHourPackageKey(model.getUserId + "", model.getHour, model.getPackageName)
      (key, model.getTimeLen)
    }

    val resultDStream: DStream[(UserHourPackageKey, Long)] = pairDStream.reduceByKey(_ + _)

    resultDStream.print()

    resultDStream.foreachRDD { (rdd: RDD[(UserHourPackageKey, Long)], time: Time) =>
      rdd.foreachPartition {
        res => {
          if (res.nonEmpty) {
            val service: BehaviorStatService = BehaviorStatService.getInstance(serverProps)

            res.foreach { r: (UserHourPackageKey, Long) =>
              val key: UserHourPackageKey = r._1
              val timeLen: Long = r._2

              val model: UserBehaviorStatModel = new UserBehaviorStatModel
              model.setUserId(MyStringUtil.getFixedLengthStr(key.userId, 10))
              model.setHour(key.hour)
              model.setPackageName(key.packageName)
              model.setTimeLen(timeLen)

              service.addTimeLen(model)
            }
          }
        }
      }

      val javaOffsetRanges : AtomicReference[Array[OffsetRange]] = new AtomicReference()
      javaOffsetRanges.set(offsetRanges)

      OffsetUtil.offsetToZk(JavaConversions.mapAsJavaMap(kafkaParams),javaOffsetRanges,groupId)
    }

    sc
  }
}
