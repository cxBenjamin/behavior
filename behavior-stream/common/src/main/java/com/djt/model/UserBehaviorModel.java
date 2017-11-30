package com.djt.model;

public class UserBehaviorModel extends BaseModel
{
  private long userId;
  private long beginTime;
  private long endTime;
  private String hour;
  private String onlyHour;
  private String packageName;
  //时长(秒)
  private long timeLen;

  public long getUserId() {
    return userId;
  }

  public void setUserId(long userId) {
    this.userId = userId;
  }

  public long getBeginTime() {
    return beginTime;
  }

  public void setBeginTime(long beginTime) {
    this.beginTime = beginTime;
  }

  public long getEndTime() {
    return endTime;
  }

  public void setEndTime(long endTime) {
    this.endTime = endTime;
  }

  public String getHour() {
    return hour;
  }

  public void setHour(String hour) {
    this.hour = hour;
  }

  public String getOnlyHour() {
    return onlyHour;
  }

  public void setOnlyHour(String onlyHour) {
    this.onlyHour = onlyHour;
  }

  public String getPackageName() {
    return packageName;
  }

  public void setPackageName(String packageName) {
    this.packageName = packageName;
  }

  public long getTimeLen() {
    return timeLen;
  }

  public void setTimeLen(long timeLen) {
    this.timeLen = timeLen;
  }
}
