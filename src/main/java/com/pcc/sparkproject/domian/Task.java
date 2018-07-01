package com.pcc.sparkproject.domian;

import java.io.Serializable;

/**
 * task表 --- domain类
 * @author 99653
 *
 */

public class Task implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = -4910194992260428332L;
	
	private long taskid;
	private String taskName;
	private String createTime;
	private String startTime;
	private String finishTime;
	private String taskType;
	private String taskStatus;
	private String taskParam;
	
	public Task(long taskid, String taskName, String createTime, String startTime, String finishTime, String taskType,
			String taskStatus, String taskParam) {
		super();
		this.taskid = taskid;
		this.taskName = taskName;
		this.createTime = createTime;
		this.startTime = startTime;
		this.finishTime = finishTime;
		this.taskType = taskType;
		this.taskStatus = taskStatus;
		this.taskParam = taskParam;
	}
	public Task() {
	}
	public long getTaskid() {
		return taskid;
	}
	public void setTaskid(long taskid) {
		this.taskid = taskid;
	}
	public String getTaskName() {
		return taskName;
	}
	public void setTaskName(String taskName) {
		this.taskName = taskName;
	}
	public String getCreateTime() {
		return createTime;
	}
	public void setCreateTime(String createTime) {
		this.createTime = createTime;
	}
	public String getStartTime() {
		return startTime;
	}
	public void setStartTime(String startTime) {
		this.startTime = startTime;
	}
	public String getFinishTime() {
		return finishTime;
	}
	public void setFinishTime(String finishTime) {
		this.finishTime = finishTime;
	}
	public String getTaskType() {
		return taskType;
	}
	public void setTaskType(String taskType) {
		this.taskType = taskType;
	}
	public String getTaskStatus() {
		return taskStatus;
	}
	public void setTaskStatus(String taskStatus) {
		this.taskStatus = taskStatus;
	}
	public String getTaskParam() {
		return taskParam;
	}
	public void setTaskParam(String taskParam) {
		this.taskParam = taskParam;
	}
	public static long getSerialversionuid() {
		return serialVersionUID;
	}

}
