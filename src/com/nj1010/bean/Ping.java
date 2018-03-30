package com.nj1010.bean;

import java.util.Date;

public class Ping {
	private Date time;
	private Double rtt;
	public Date getTime() {
		return time;
	}
	public void setTime(Date time) {
		this.time = time;
	}
	public Double getRtt() {
		return rtt;
	}
	public void setRtt(Double rtt) {
		this.rtt = rtt;
	}

}
