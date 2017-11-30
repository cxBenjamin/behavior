package com.djt.model;

import org.codehaus.jackson.annotate.JsonProperty;

public class SingleUserBehaviorRequestModel extends BaseModel
{
  private static final long serialVersionUID = 1L;

  @JsonProperty("package")
  private String packageName;

  @JsonProperty("activetime")
  private long activeTime;

  public String getPackageName()
  {
    return packageName;
  }

  public void setPackageName(String packageName)
  {
    this.packageName = packageName;
  }

  public long getActiveTime()
  {
    return activeTime;
  }

  public void setActiveTime(long activeTime)
  {
    this.activeTime = activeTime;
  }
}
