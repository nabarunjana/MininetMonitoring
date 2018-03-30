package com.nj1010.core;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Scanner;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import com.nj1010.bean.*;
import com.nj1010.dao.DAO;
import com.nj1010.util.Reader;

public class Main {

	public static File folder = new File(".");
	static Scanner sc = new Scanner(System.in);
	private static DAO dao = new DAO();
	public static void main(String[] args) {
		if (args.length > 0) {
			if (args[0].matches("load"))
				loadAllFiles(folder);
			if (args[0].matches("insert")) {
				if (args.length>1 && args[1].matches("[0-9]+")) {
					dao.setBatchId(Long.parseLong(args[1]));
					loadAllFiles(folder);
				}
				else dbInsert(args);
			}
			if (args[0].matches("zip")) unzipAndLoad(args[1]);
			if (args[0].matches("delete")) dbDelete(args);
			if (args[0].matches("truncate")) dbTruncate();
			if (args[0].matches("drop")) dropTables();
			if (args[0].matches("create")) dbOps();
		}
		else
			displayHelp();
	}

	private static void dropTables() {
		System.out.println("Are you sure you want to drop all tables?");
		if(sc.nextLine().toUpperCase().matches("Y"))
		    dao.drop();
		sc.close();
	}

	private static void displayHelp() {
		System.out.println("Run with arguments as below:");
		System.out.println("\t create \t \t - to create db schema with tables and views");
		System.out.println("\t load \t \t \t - to load all files");
		System.out.println("\t zip [file.zip] \t - to unzip and load all files");
		System.out.println("\t delete [BATCH_ID] \t - to delete all data for that BATCH_ID");
		System.out.println("\t insert [tbl] [vls] \t - to insert vls into tbl");
		System.out.println("\t truncate \t \t - to truncate all tables");
		System.out.println("\t drop \t \t \t - to drop schema with tables and views");
	}

	private static void unzipAndLoad(String name) {
		ZipFile z;
		File tempFolder = new File("temp"+System.currentTimeMillis());
		tempFolder.mkdir();
		try {
			z = new ZipFile(name);
			Enumeration<? extends ZipEntry> it = z.entries();
			while (it.hasMoreElements()) {
				ZipEntry el = it.nextElement();
				InputStream zis = z.getInputStream(el);
				FileOutputStream op=new FileOutputStream(tempFolder.getAbsolutePath()+File.separator+el.getName());
				while(zis.available()>0)
					op.write(zis.read());
				op.close();
			}
		} catch (IOException e) {e.printStackTrace();}
		loadAllFiles(tempFolder);
	}

	private static void dbTruncate() {
		System.out.println("Are you sure you want to truncate all tables?");
		if(sc.nextLine().toUpperCase().matches("Y"))
		    dao.truncate();
		sc.close();
	}

	private static void dbDelete(String[] args) {
		System.out.println("Are you sure you want to delete "+args[1]+"?");
		if(sc.nextLine().toUpperCase().matches("Y"))
		    dao.delete(args);
		sc.close();
	}
	private static void dbInsert(String[] args) {
	    dao.insert(args);
	}
	public static void dbOps() {
	    dao.createTables();
	    dao.createViews();
	}
	public static void loadAllFiles(final File folder) {
		Reader r = new Reader();
		ArrayList<Iperf3Stat> iperf3 = new ArrayList<Iperf3Stat>();
		ArrayList<DeviceStat> devStats = new ArrayList<DeviceStat>();
		ArrayList<DeviceStat> controllerStat = new ArrayList<DeviceStat>();
		ArrayList<PortStat> portStat = new ArrayList<PortStat>();
		ArrayList<RTT> rtt = new ArrayList<RTT>();
		ArrayList<Ping> ping =new ArrayList<Ping>();
		ArrayList<ArrayList<Ping>> pings = new ArrayList<ArrayList<Ping>>() ;
		ArrayList<ArrayList<Iperf3Stat>> al = new ArrayList<ArrayList<Iperf3Stat>>();
		ArrayList<String> pairs = new ArrayList<String>();
		ArrayList<String> pingPairs = new ArrayList<String>();
		ArrayList<Coefficient> coefs = new ArrayList<Coefficient>();
		int blocked=0;
	    for (File fileEntry : folder.listFiles()) {
	        if (!fileEntry.isDirectory()) {
				if(fileEntry.getName().contains("iperf")) {
					iperf3 = r.readIperf3Dat(fileEntry.getAbsolutePath());
					pairs.add(fileEntry.getName().split("\\.")[0]);
					al.add(iperf3);
				}
				else if(fileEntry.getName().contains("DevStat")) {
					devStats = r.readDevStats(fileEntry.getAbsolutePath());
				}
				else if(fileEntry.getName().contains("ControllerStat")) {
					controllerStat = r.readDevStats(fileEntry.getAbsolutePath());
				}
				else if(fileEntry.getName().contains("IfOutStats")) {
					portStat = r.readIfOutStats(fileEntry.getAbsolutePath());
					portStat = r.readPortNumbers("ports.txt", portStat);
				}
				else if(fileEntry.getName().contains("ping.txt") && fileEntry.getName().contains("-")) {
					ping = r.readRepPingStats(fileEntry.getAbsolutePath());
					pingPairs.add(fileEntry.getName().split("\\.")[0]);
					pings.add(ping);
				}
				else if(fileEntry.getName().contains("ping") && fileEntry.getName().contains("txt")) {
					rtt = r.readPingStats(fileEntry.getAbsolutePath());
				}
				else if(fileEntry.getName().contains("coef") && fileEntry.getName().contains("txt")) {
					coefs = r.readCoefficients(fileEntry.getAbsolutePath());
				}
				else if(fileEntry.getName().contains("blocked")) {
					blocked = Integer.parseInt(r.read(fileEntry.getAbsolutePath()));
				}
	        }
	    }
	    int rowsDevStats = 0,rowsControllerStats = 0,rowsPortStats = 0,rowsPingStats = 0,rowsPingTotal=0, rowsIperf3Total=0,idx=0,rowsCoefficients = 0;
	    int[] rowsIperf3 = new int[pairs.size()];
	    int[] rowsPing = new int[pingPairs.size()];
	    if (devStats.size()>0) rowsDevStats = dao.insertDevStats(devStats);
	    System.out.println("Dev stats: "+ rowsDevStats);
	    if (controllerStat.size()>0) rowsControllerStats = dao.insertControllerStats(controllerStat);
	    System.out.println("Controller stats: "+ rowsControllerStats);
	    if (portStat.size()>0) rowsPortStats = dao.insertPortStats(portStat);
	    System.out.println("Port stats: "+ rowsPortStats);
	    if (rtt.size()>0) rowsPingStats = dao.insertPingStats(rtt);
	    System.out.println("Ping stats: "+ rowsPingStats);
	    if (coefs.size()>0) rowsCoefficients = dao.insertCoeffiecients(coefs);
	    System.out.println("Coefficients: "+rowsCoefficients);
	    Iterator<ArrayList<Ping>> pingRepList = pings.iterator();
	    Iterator<String> pingPairIt = pingPairs.iterator();
	    String pingPair; ArrayList<Ping> pingRep;
	    while(pingPairIt.hasNext()){
	    	pingPair = pingPairIt.next();
	    	pingRep = pingRepList.next();
	    	rowsPing[idx] = dao.insertPingRep(pingRep, pingPair);
	    	rowsPingTotal += rowsPing[idx];
		    System.out.println("Ping stats for "+ pingPair +": " + rowsPing[idx++]);
	    }
	    System.out.println("Total Ping stats : "+rowsPingTotal);
	    idx=0;
	    Iterator<ArrayList<Iperf3Stat>> iperf3ListIt = al.iterator();
	    Iterator<String> pairIt = pairs.iterator();
	    String pair;ArrayList<Iperf3Stat> obj;
	    while(pairIt.hasNext()) {
	    	pair = pairIt.next();
	    	obj = iperf3ListIt.next();
	    	rowsIperf3Total += rowsIperf3[idx] = dao.insertIperf3Stats(obj, pair);;
		    System.out.println("Iperf3 stats for "+ pair +": " + rowsIperf3[idx++]);
	    }
	    dao.batch_run(rowsDevStats, rowsControllerStats, rowsPortStats, rowsIperf3Total,rowsPingTotal,rowsCoefficients,blocked);
	    System.out.println(dao.getBatchId());
	}
	public static boolean checkDateLesser(Date dt1,Date dt2) {
		return dt1.getTime()<dt2.getTime();
	}
}
