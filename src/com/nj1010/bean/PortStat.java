package com.nj1010.bean;

import java.util.Date;

public class PortStat {
	private Date time;
	private String name;
	private int portNo;
	private long outPackets;
	private long diffPackets;

	public Date getTime() {
		return time;
	}
	public void setTime(Date time) {
		this.time = time;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public int getPortNo() {
		return portNo;
	}
	public void setPortNo(int portNo) {
		this.portNo = portNo;
	}
	public long getOutPackets() {
		return outPackets;
	}
	public void setOutPackets(long outPackets) {
		this.outPackets = outPackets;
	}
	public long getDiffPackets() {
		return diffPackets;
	}
	public void setDiffPackets(long diffPackets) {
		this.diffPackets = diffPackets;
	}

}
