package com.djt.model;

import org.codehaus.jackson.annotate.JsonProperty;

import java.util.List;

public class UserBehavorRequestModel extends BaseModel {
	@JsonProperty("begintime")
	private Long beginTime;

	@JsonProperty("endtime")
	private Long endTime;

	@JsonProperty("data")
	private List<SingleUserBehaviorRequestModel> singleUserBehaviorRequestModelList;

	private long userId;

	private String day;

	public Long getBeginTime() {
		return beginTime;
	}

	public void setBeginTime(Long beginTime) {
		this.beginTime = beginTime;
	}

	public Long getEndTime() {
		return endTime;
	}

	public void setEndTime(Long endTime) {
		this.endTime = endTime;
	}

	public List<SingleUserBehaviorRequestModel> getSingleUserBehaviorRequestModelList() {
		return singleUserBehaviorRequestModelList;
	}

	public void setSingleUserBehaviorRequestModelList(List<SingleUserBehaviorRequestModel> singleUserBehaviorRequestModelList) {
		this.singleUserBehaviorRequestModelList = singleUserBehaviorRequestModelList;
	}

	public long getUserId() {
		return userId;
	}

	public void setUserId(long userId) {
		this.userId = userId;
	}

	public String getDay() {
		return day;
	}

	public void setDay(String day) {
		this.day = day;
	}
}
