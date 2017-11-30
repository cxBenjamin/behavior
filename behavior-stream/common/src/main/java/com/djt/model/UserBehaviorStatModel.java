package com.djt.model;

public class UserBehaviorStatModel extends BaseModel
{
  private String userId;
  private String hour;
  private String packageName;
  private long timeLen;

  public String getUserId() {
    return userId;
  }

  public void setUserId(String userId) {
    this.userId = userId;
  }

  public String getHour() {
    return hour;
  }

  public void setHour(String hour) {
    this.hour = hour;
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
