package com.djt.spark.service;

import com.djt.model.UserBehaviorStatModel;
import com.djt.spark.common.HBaseClient;
import com.djt.utils.DateUtils;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import java.util.Properties;

public class BehaviorStatService
{
  private Properties props;
  private static BehaviorStatService service;

  public static BehaviorStatService getInstance(Properties props) {
    if (service == null) {
      synchronized (BehaviorStatService.class) {
        if (service == null) {
          service = new BehaviorStatService();
          service.props = props;
        }
      }
    }

    return service;
  }

  /*
  * 时长统计
  * */
  public void addTimeLen(UserBehaviorStatModel model) {
    addUserBehaviorList(model);
    addUserHourTimeLen(model);
    addUserDayTimeLen(model);
    addUserPackageHourTimeLen(model);
    addUserPackageDayTimeLen(model);
  }

  /*
  * 用户使用过哪些APP和使用时长
  * */
  public void addUserBehaviorList(UserBehaviorStatModel model) {
    String tableName = "behavior_user_app_" + DateUtils.getMonthByHour(model.getHour());
    Table table = HBaseClient.getInstance(this.props).getTable(tableName);
    String rowKey = model.getUserId()+":"+DateUtils.getDayByHour(model.getHour());

    try {
      table.incrementColumnValue(Bytes.toBytes(rowKey), Bytes.toBytes("timeLen"), Bytes.toBytes(model.getPackageName()), model.getTimeLen());
    } catch (Exception ex) {
      HBaseClient.closeTable(table);
      ex.printStackTrace();
    }
  }

  /*
  * 用户每小时的使用应用的时长
  * */
  public void addUserHourTimeLen(UserBehaviorStatModel model) {
    String tableName = "behavior_user_hour_time_" + DateUtils.getMonthByHour(model.getHour());
    Table table = HBaseClient.getInstance(this.props).getTable(tableName);
    String rowKey = model.getUserId()+":"+DateUtils.getDayByHour(model.getHour());

    try {
      table.incrementColumnValue(Bytes.toBytes(rowKey), Bytes.toBytes("timeLen"), Bytes.toBytes(DateUtils.getOnlyHourByHour(model.getHour())), model.getTimeLen());
    } catch (Exception ex) {
      HBaseClient.closeTable(table);
      ex.printStackTrace();
    }
  }

  /*
  * 用户每天的玩机时长
  * */
  public void addUserDayTimeLen(UserBehaviorStatModel model) {
    String tableName = "behavior_user_day_time_" + DateUtils.getMonthByHour(model.getHour());
    Table table = HBaseClient.getInstance(this.props).getTable(tableName);
    String rowKey = String.valueOf(model.getUserId());

    try {
      table.incrementColumnValue(Bytes.toBytes(rowKey), Bytes.toBytes("timeLen"), Bytes.toBytes(DateUtils.getOnlyDayByHour(model.getHour())), model.getTimeLen());
    } catch (Exception ex) {
      HBaseClient.closeTable(table);
      ex.printStackTrace();
    }
  }

  /*
  * 用户每个应用每小时的玩机时长
  * */
  public void addUserPackageHourTimeLen(UserBehaviorStatModel model) {
    String tableName = "behavior_user_hour_app_time_" + DateUtils.getMonthByHour(model.getHour());
    Table table = HBaseClient.getInstance(this.props).getTable(tableName);
    String rowKey = model.getUserId()+":"+DateUtils.getDayByHour(model.getHour())+":"+model.getPackageName();

    try {
      table.incrementColumnValue(Bytes.toBytes(rowKey), Bytes.toBytes("timeLen"), Bytes.toBytes(DateUtils.getOnlyHourByHour(model.getHour())), model.getTimeLen());
    } catch (Exception ex) {
      HBaseClient.closeTable(table);
      ex.printStackTrace();
    }
  }

  /*
  * 用户每个应用每天的玩机时长
  * */
  public void addUserPackageDayTimeLen(UserBehaviorStatModel model) {
    String tableName = "behavior_user_day_app_time_" + DateUtils.getMonthByHour(model.getHour());
    Table table = HBaseClient.getInstance(this.props).getTable(tableName);
    String rowKey = model.getUserId()+":"+model.getPackageName();

    try {
      table.incrementColumnValue(Bytes.toBytes(rowKey), Bytes.toBytes("timeLen"), Bytes.toBytes(DateUtils.getOnlyDayByHour(model.getHour())), model.getTimeLen());
    } catch (Exception ex) {
      HBaseClient.closeTable(table);
      ex.printStackTrace();
    }
  }
}
