package com.nj1010.util;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import com.nj1010.bean.*;

public class Reader {
	private SimpleDateFormat sdf = new SimpleDateFormat("hh:mm:ss");
	private Date time;String line;double multiplier=0;
	private BufferedReader br;
	private static int dropped =0;
	
	public static int getDropped() {return dropped;}
	private double getMultiplier(String data) {
		switch (data) {
			case "G": multiplier = 1e9; break;
			case "M": multiplier = 1e6; break;
			case "K": multiplier = 1e3; break;
			default: multiplier = 1; break;
		}
		return multiplier;
	}
	private String clean(String text) {
		return text.trim().replaceAll(" +", " ");
	}
	public ArrayList<Iperf3Stat> readIperf3Dat(String f) {
		Iperf3Stat iperf3stat;double bandwidth,transfer;
		String data;//,sec;
		ArrayList<Iperf3Stat> al = new  ArrayList<Iperf3Stat>();
		try {
			br = new BufferedReader(new FileReader(f));
			int skip=6;
			if(f.contains("iperf3")) skip = 3;
            while(skip>0) {
            	line=br.readLine();
            	if(line==null) {dropped++;break;}
            	skip--;
            }
            Calendar cal ;
			while ((line = br.readLine()) != null) {
				cal = Calendar.getInstance();
				if (line.contains("- -")) break;
				iperf3stat = new Iperf3Stat();
				line = clean(line);
				time = sdf.parse(line.split(" ")[0]);
				cal.setTime(time);
				//sec = line.split(" ")[3].split("-")[0];
				//cal.add(Calendar.SECOND, (int)Double.parseDouble(sec));
				iperf3stat.setTime(cal.getTime());
				if(line.contains("----")) {
					skip=6;
					if(f.contains("iperf3")) skip = 3;
		            while(skip>0) {
		            	line=br.readLine();
		            	skip--;
		            }
				}
				line=clean(line);	
				data = line.split("sec")[1].trim();
				multiplier = getMultiplier(data.split(" ")[1].substring(0,1));
				transfer = Double.parseDouble(data.split(" ")[0]) * multiplier;
				multiplier = getMultiplier(data.split(" ")[3].substring(0,1));
				bandwidth = Double.parseDouble(data.split(" ")[2]) * multiplier;
				
				iperf3stat.setTransfer((int)transfer);
				iperf3stat.setBandwidth((int)bandwidth);
				al.add(iperf3stat);

			}
		}
		catch (Exception e){
			System.out.println(f + " "  + line + line.split("sec")[1].trim().split(" ").length);
			e.printStackTrace();}
		return al;

	}

	public ArrayList<PortStat> readIfOutStats(String f) {
		PortStat portStat;
		ArrayList<PortStat> al = new  ArrayList<PortStat>();
		HashMap<Integer,Long> packets = new HashMap<Integer,Long>();
		try {
			br = new BufferedReader(new FileReader(f));
			while ((line = br.readLine()) != null) {
				if (line.matches("[0-9][0-9]:[0-9][0-9]:[0-9][0-9]")) {
					time = sdf.parse(line);
					continue;
				}
				line = clean(line);
				portStat = new PortStat();
				portStat.setTime(time);
				portStat.setPortNo(Integer.parseInt(line.split(":")[2].split(" ")[0].split("\\.")[1]));
				portStat.setOutPackets(Long.parseLong(line.split(":")[3].trim()));
				Long old = packets.get(portStat.getPortNo());
				if (old == null) old = (long) 0;
				portStat.setDiffPackets(portStat.getOutPackets()-old);
				if(!packets.containsKey(portStat.getPortNo()))
					packets.put(portStat.getPortNo(),portStat.getOutPackets());
				else
					packets.replace(portStat.getPortNo(),portStat.getOutPackets());
				al.add(portStat);
				}
		}
		catch (Exception e){
			System.out.println(f + " "  + line);
			e.printStackTrace();}
		return al;
	}
	public ArrayList<PortStat> readPortNumbers(String f,ArrayList<PortStat> al) {
		PortStat portStat;
		Iterator<PortStat> it = al.iterator();
		HashMap<Integer, String> hmap = new HashMap<Integer, String>();
		try {
			br = new BufferedReader(new FileReader(f));
			while ((line = br.readLine()) != null) {
				line = clean(line);
				if(line.matches("[0-9].*")) {
					hmap.put(Integer.parseInt(line.split(":")[0].trim()), line.split(":")[1]);
				}
			}
			while(it.hasNext()) {
				portStat = it.next();
				portStat.setName(hmap.get(portStat.getPortNo()));
			}
		}
		catch (FileNotFoundException e){System.out.println("Ports.txt not found");}
		catch (Exception e){
			System.out.println(f + " "  + line);
			e.printStackTrace();}
		return al;
	}
	
	public ArrayList<DeviceStat> readDevStats(String f) {
		DeviceStat deviceStat;
		ArrayList<DeviceStat> al = new  ArrayList<DeviceStat>();
		try {
			br = new BufferedReader(new FileReader(f));            
			while ((line = br.readLine()) != null) {
				deviceStat = new DeviceStat();
				line = clean(line);
				time = sdf.parse(line);
				line = br.readLine();
				deviceStat.setTime(time);
				deviceStat.setRamUsage(Integer.parseInt(line.split(":")[3].trim()));
				line = br.readLine();
				int cpu = Integer.parseInt(line.split(":")[3].trim());
				line = br.readLine();
				cpu += Integer.parseInt(line.split(":")[3].trim());
				deviceStat.setCpuUsage(cpu/2);
				al.add(deviceStat);
				}
		}
		catch (Exception e){
			System.out.println(f + " "  + line);
			e.printStackTrace();}
		return al;
	}
	
	public ArrayList<RTT> readPingStats(String f) {
		RTT rtt;
		ArrayList<RTT> al = new  ArrayList<RTT>();
		try {
			br = new BufferedReader(new FileReader(f));            
			while ((line = br.readLine()) != null) {
				rtt = new RTT();
				line = clean(line);
				time = sdf.parse(line);
				line = br.readLine();
				rtt.setTime(time);
				if(!line.contains("rtt"))
					break;
				rtt.setMin(Double.parseDouble(line.split(" ")[3].split("/")[0].trim()));
				rtt.setAvg(Double.parseDouble(line.split(" ")[3].split("/")[1].trim()));
				rtt.setMax(Double.parseDouble(line.split(" ")[3].split("/")[2].trim()));
				al.add(rtt);
				}
		}
		catch (Exception e){
			System.out.println(f + " "  + line);
			e.printStackTrace();}
		return al;
	}
	public ArrayList<Ping> readRepPingStats(String f) {
		Ping pingRTT;
		ArrayList<Ping> al = new  ArrayList<Ping>();
		//String sec;
		try {
			br = new BufferedReader(new FileReader(f));   
			int skip = 1;
            while(skip>0) {
            	line=br.readLine();
            	if(line==null) {dropped++;break;}
            	skip--;
            }
			Calendar cal = Calendar.getInstance();
			while ((line = br.readLine()) != null) {
				if (line.contains("icmp")) {
					if(line.contains("Unreachable")) {dropped++;break;}
					pingRTT = new Ping();
					line = clean(line);
					time = sdf.parse(line.split(" ")[0]);
					cal.setTime(time);
					//sec = line.split(" ")[5].split("=")[1];
					//cal.add(Calendar.SECOND, (int)Double.parseDouble(sec)*10-10);
					pingRTT.setTime(cal.getTime());
					pingRTT.setRtt(Double.parseDouble(line.split(" ")[7].split("=")[1].trim()));
					al.add(pingRTT);
					}
				}
		}
		catch (Exception e){
			System.out.println(f + " "  + line);
			e.printStackTrace();}
		return al;
	}
	public ArrayList<Coefficient> readCoefficients(String f) {
		Coefficient coeff;
		int serialNumber=0;
		ArrayList<Coefficient> al = new ArrayList<Coefficient>();
		try {
			br = new BufferedReader(new FileReader(f));
			while ((line = br.readLine()) != null) {
				coeff = new Coefficient();
				coeff.setCoefficient(Double.parseDouble(line));
				serialNumber++;
				coeff.setSerialNumber(serialNumber);
				al.add(coeff);
			}
		}catch (Exception e){
			System.out.println(f + " "  + line);
			e.printStackTrace();}
		return al;
	}
	public String read(String f) {
		String ans = "";
		try {
			br = new BufferedReader(new FileReader(f));
			while ((line = br.readLine()) != null) 
				ans+=line;
		}catch (Exception e){
			System.out.println(f + " "  + line);
			e.printStackTrace();}
		return ans;
	}
}
