package com.nj1010.bean;

import java.util.Date;

public class DeviceStat {
	private Date time;
	private int ramUsage;
	private int cpuUsage;
	
	public Date getTime() {
		return time;
	}
	public void setTime(Date time) {
		this.time = time;
	}
	public int getRamUsage() {
		return ramUsage;
	}
	public void setRamUsage(int ramUsage) {
		this.ramUsage = ramUsage;
	}
	public int getCpuUsage() {
		return cpuUsage;
	}
	public void setCpuUsage(int cpuUsage) {
		this.cpuUsage = cpuUsage;
	}

}
