package com.nj1010.dao;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;
import com.nj1010.bean.*;
import com.nj1010.util.Reader;

public class DAO {

	private static long BATCH_ID = new java.util.Date().getTime(); 
	DBProperties dbp = readProperties();
	String[] tables = {"pingRepStat","Iperf3Stat","batch_run","coefficients","sessionMap","pingStat","PortStat","ControllerStat","DevStat"};
	public long getBatchId() {
		return BATCH_ID;
	}
	public void setBatchId(long BATCH_ID) {
		DAO.BATCH_ID = BATCH_ID;
	}
	private DBProperties readProperties() {
		DBProperties dbp = new DBProperties();
		Properties p = new Properties();
		try {p.load(new FileInputStream("dbcon.properties"));}
		catch (IOException e1) {e1.printStackTrace();}
		dbp.setDbserver(p.getProperty("dbserver"));
		dbp.setHost(p.getProperty("host"));
		dbp.setDatabase(p.getProperty("database"));
		dbp.setUser(p.getProperty("user"));
		dbp.setPassword(p.getProperty("password"));
		return dbp;
	}
	private Connection getConnection() {
		Connection con = null;String dbserver,database;
		dbserver = dbp.getDbserver();
		database = dbp.getDatabase();
		try {con = DriverManager.getConnection("jdbc:"+dbserver+"://" +dbp.getHost() 
		+ (dbserver.equals("mariadb")? "/" + database:"; database="+database), dbp.getUser(), dbp.getPassword());}
		catch (SQLException e) {e.printStackTrace();}
		return con;
	}
	public void createTables() {
		Connection con = getConnection();
		try {
			Statement stmt=con.createStatement();
			stmt.execute("CREATE TABLE DevStat (BATCH_ID BIGINT ,time TIME, ramUsage INTEGER,cpuUsage INTEGER)");
			stmt.execute("CREATE TABLE ControllerStat (BATCH_ID BIGINT ,time TIME, ramUsage INTEGER, cpuUsage INTEGER)");
			stmt.execute("CREATE TABLE PortStat (BATCH_ID BIGINT ,time TIME, Name VARCHAR(20), PortNo INTEGER, OutPackets BIGINT)");
			stmt.execute("CREATE TABLE Iperf3Stat (BATCH_ID BIGINT ,time TIME, transfer INTEGER, bandwidth BIGINT,pair VARCHAR(20))");
			stmt.execute("CREATE TABLE pingStat (BATCH_ID BIGINT ,time TIME, min INTEGER, avg INTEGER, max INTEGER)");
			stmt.execute("CREATE TABLE batch_run (BATCH_ID BIGINT ,rDevStats INTEGER,rContrStats INTEGER, rPortStats INTEGER, rIperf3Stats INTEGER, rPingRepStats INTEGER, coefficient INTEGER, dropped INTEGER, blocked INTEGER)");
			stmt.execute("CREATE TABLE pingRepStat (BATCH_ID BIGINT ,time TIME, rtt INTEGER, pingPair VARCHAR(20))");
			stmt.execute("CREATE TABLE coefficients (BATCH_ID BIGINT, serialNumber INTEGER, coeff NUMERIC(20,18))");
			stmt.execute("CREATE TABLE sessionMap (BATCH_ID BIGINT, session VARCHAR(20), toUse VARCHAR(20), coeff NUMERIC(20,18), slaDel INTEGER,slaBW INTEGER, bandwidth INTEGER);");
		} catch (SQLException e) {e.printStackTrace();}
		
	}
	public void createViews() {
		Connection con = getConnection();
		try {
			Statement stmt=con.createStatement();
			if (dbp.getDbserver().matches("sqlserver")) {
				stmt.execute("CREATE FUNCTION roundTime(@time TIME) returns TIME AS BEGIN DECLARE @ret TIME; SELECT @ret = CAST(DATEADD(s,ROUND(DATEDIFF(second,0,@time)/5.0,0)*5,0) AS TIME) RETURN CONVERT(TIME,@ret, 108);  END;");
				stmt.execute("CREATE VIEW vIperf3Stats as select batch_id,dbo.roundTime(time) AS roundedTime,transfer,bandwidth,pair from iperf3stat;");
				stmt.execute("CREATE VIEW vDevStats as select batch_id,dbo.roundTime(time) AS roundedTime,ramUsage,cpuUsage from DevStat;");
				stmt.execute("CREATE VIEW vControllerStats as select batch_id,dbo.roundTime(time) AS roundedTime,ramUsage,cpuUsage from ControllerStat;");
				stmt.execute("CREATE VIEW vPortStats as select batch_id,dbo.roundTime(time) AS roundedTime,Name,PortNo,OutPackets from PortStat;");
				stmt.execute("CREATE VIEW vPingStats as select batch_id,dbo.roundTime(time) AS roundedTime,min(min) as min,avg(avg) as avg,max(max) as max from pingStat group by batch_id,dbo.roundTime(time);");
				stmt.execute("CREATE VIEW vPingRepStats as select batch_id,dbo.roundTime(time) AS roundedTime,rtt, pingPair from pingRepStat;");
				
				stmt.execute("CREATE FUNCTION bootTime(@time TIME,@batch_id BIGINT) returns TIME AS BEGIN DECLARE @ret TIME, @duration INT, @text VARCHAR(20); SELECT @text = (select session from sessionMap where BATCH_ID=@batch_id); if count(@text) > 0 begin SELECT @duration = substring(@text,1,CHARINDEX('x',@text)-1) end else begin SELECT @duration = 0 end; SELECT @ret = CAST(DATEADD(s,@duration,@time) AS TIME) RETURN CONVERT(TIME,@ret, 108); END;");
				stmt.execute("CREATE FUNCTION endTime(@time TIME,@batch_id BIGINT) returns TIME AS BEGIN DECLARE @ret TIME, @duration INT, @text VARCHAR(20); SELECT @text = (select session from sessionMap where BATCH_ID=@batch_id); if count(@text) > 0 begin SELECT @duration = substring(@text,1,CHARINDEX('x',@text)-1) end else begin SELECT @duration = 0 end SELECT @ret = CAST(DATEADD(s,-@duration,@time) AS TIME) RETURN CONVERT(TIME,@ret, 108); END;");
				stmt.execute("CREATE VIEW vvIperf3Stats as select *, CASE WHEN bandwidth>500000 THEN 1 ELSE 0 END AS bw500, CASE WHEN bandwidth>1000000 THEN 1 ELSE 0 END AS bw1000, CASE WHEN bandwidth>2000000 THEN 1 ELSE 0 END AS bw2000 from vIperf3Stats a where roundedTime > (select dbo.bootTime(min(roundedTime),batch_id) from vIperf3Stats b where b.batch_id = a.batch_id group by b.BATCH_ID) and roundedTime < (select dbo.endTime(max(roundedTime),batch_id) from vIperf3Stats c where c.batch_id = a.batch_id  group by c.BATCH_ID);");
				stmt.execute("CREATE VIEW vvPingRepStats as select *, CASE WHEN rtt<100 THEN 1 ELSE 0 END AS del50, CASE WHEN rtt<200 THEN 1 ELSE 0 END AS del100, CASE WHEN rtt<400 THEN 1 ELSE 0 END AS del200 from vPingRepStats a where roundedTime > (select dbo.bootTime(min(roundedTime),batch_id) from vPingRepStats b where b.batch_id = a.batch_id group by b.BATCH_ID) and roundedTime < (select dbo.endTime(max(roundedTime),batch_id) from vPingRepStats c where c.batch_id = a.batch_id  group by c.BATCH_ID);");
				
				stmt.execute("CREATE VIEW DelaySLA AS SELECT batch_id,min(rtt) as Min,max(rtt) as Max,avg(rtt) as Avg,count(*) as Total,sum(del50) as del50,sum(del100) as del100,sum(del200) as del200 FROM vvPingRepStats GROUP BY batch_id;");
				stmt.execute("CREATE VIEW BandwidthSLA AS SELECT batch_id,min(bandwidth) as Min,max(bandwidth) as Max,avg(bandwidth) as Avg,count(*) as Total,sum(bw500) as bw500,sum(bw1000) as bw1000,sum(bw2000) as bw2000 FROM vvIperf3Stats GROUP BY batch_id;");
				stmt.execute("CREATE VIEW CombinedSLAs AS SELECT vvPingRepStats.BATCH_ID,vvPingRepStats.roundedTime, CASE WHEN del50=bw2000 AND bw2000=1 THEN 1 ELSE 0 END AS HSLA, CASE WHEN del100=bw1000 AND bw1000=1 THEN 1 ELSE 0 END AS MSLA, CASE WHEN del200=bw500 AND bw500=1 THEN 1 ELSE 0 END AS LSLA from vvPingRepStats inner join vvIperf3Stats on vvPingRepStats.BATCH_ID = vvIperf3Stats.BATCH_ID and vvPingRepStats.roundedTime=vvIperf3Stats.roundedTime and vvIperf3Stats.pair=vvPingRepStats.pingPair;");
			}
			if (dbp.getDbserver().matches("maria")) {
				stmt.execute("CREATE VIEW vIperf3Stats as select batch_id,SEC_TO_TIME(ROUND(TIME_TO_SEC(time)/10)*10) AS roundedTime,transfer,bandwidth,pair from iperf3stat;");
				stmt.execute("CREATE VIEW vDevStats as select batch_id,SEC_TO_TIME(ROUND(TIME_TO_SEC(time)/10)*10) AS roundedTime,ramUsage,cpuUsage from DevStat;");
				stmt.execute("CREATE VIEW vControllerStats as select batch_id,SEC_TO_TIME(ROUND(TIME_TO_SEC(time)/10)*10) AS roundedTime,ramUsage,cpuUsage from ControllerStat;");
				stmt.execute("CREATE VIEW vPortStats as select batch_id,SEC_TO_TIME(ROUND(TIME_TO_SEC(time)/10)*10) AS roundedTime,Name,PortNo,OutPackets from PortStat;");
				stmt.execute("CREATE VIEW vPingStats as select batch_id,SEC_TO_TIME(ROUND(TIME_TO_SEC(time)/10)*10) AS roundedTime,min(min) as min,avg(avg) as avg,max(max) as max from pingStat group by roundedTime;");
				stmt.execute("CREATE VIEW vPingRepStats as select batch_id,SEC_TO_TIME(ROUND(TIME_TO_SEC(time)/10)*10) AS roundedTime,rtt, pingPair from pingRepStat;");
				
				stmt.execute("CREATE VIEW vis as (select * from vIperf3Stats where batch_id = (select max(batch_id) from vIperf3Stats));");
				stmt.execute("CREATE VIEW vds as (select * from vDevStats where batch_id = (select max(batch_id) from vDevStats));");
				stmt.execute("CREATE VIEW vcs as (select * from vControllerStats where batch_id = (select max(batch_id) from vControllerStats));");
				stmt.execute("CREATE VIEW vps as (select * from vPortStats where batch_id = (select max(batch_id) from vPortStats));");
				stmt.execute("CREATE VIEW vgs as (select * from vPingStats where batch_id = (select max(batch_id) from vPingStats));");
			}
		} catch (SQLException e) {e.printStackTrace();}
		
	}
	public int insertDevStats(ArrayList<DeviceStat> devStats) {
		int rows = 0;
		Connection con = getConnection();
		Iterator<DeviceStat> it = devStats.iterator();
		try {
			while(it.hasNext()) {
				DeviceStat obj = it.next();
				PreparedStatement stmt = con.prepareStatement(
						"INSERT INTO DevStat VALUES(?,?,?,?)");
				stmt.setLong(1, BATCH_ID);
				stmt.setTime(2, new Time(obj.getTime().getTime()));
				stmt.setInt(3, obj.getRamUsage());
				stmt.setInt(4, obj.getCpuUsage());
				rows += stmt.executeUpdate();
			}
		} catch (SQLException e) {e.printStackTrace();}
		finally {try {con.close();} catch (SQLException e) {e.printStackTrace();}}
		return rows;
	}
	public int insertControllerStats(ArrayList<DeviceStat> devStats) {
		int rows = 0;
		Connection con = getConnection();
		Iterator<DeviceStat> it = devStats.iterator();
		try {
			while(it.hasNext()) {
				DeviceStat obj = it.next();
				PreparedStatement stmt = con.prepareStatement(
						"INSERT INTO ControllerStat VALUES(?,?,?,?)");
				stmt.setLong(1, BATCH_ID);
				stmt.setTime(2, new Time(obj.getTime().getTime()));
				stmt.setInt(3, obj.getRamUsage());
				stmt.setInt(4, obj.getCpuUsage());
				rows += stmt.executeUpdate();
			}
		} catch (SQLException e) {e.printStackTrace();}
		finally {try {con.close();} catch (SQLException e) {e.printStackTrace();}}
		return rows;
	}
	public int insertPortStats(ArrayList<PortStat> portStats) {
		int rows = 0;
		Connection con = getConnection();
		Iterator<PortStat> it = portStats.iterator();
		try {
			while(it.hasNext()) {
				PortStat obj = it.next();
				PreparedStatement stmt = con.prepareStatement(
						"INSERT INTO portStat VALUES(?,?,?,?,?,?)");
				stmt.setLong(1, BATCH_ID);
				stmt.setTime(2, new Time(obj.getTime().getTime()));
				stmt.setString(3, obj.getName());
				stmt.setInt(4, obj.getPortNo());
				stmt.setLong(5, obj.getOutPackets());
				stmt.setLong(6, obj.getDiffPackets());
				rows += stmt.executeUpdate();
			}
		} catch (SQLException e) {e.printStackTrace();}
		finally {try {con.close();} catch (SQLException e) {e.printStackTrace();}}
		return rows;
	}
	public int insertIperf3Stats(ArrayList<Iperf3Stat> iperf3Stats,String pair) {
		int rows = 0;
		Connection con = getConnection();
		Iterator<Iperf3Stat> it = iperf3Stats.iterator();
		try {
			while(it.hasNext()) {
				Iperf3Stat obj = it.next();
				PreparedStatement stmt = con.prepareStatement(
						"INSERT INTO iperf3Stat VALUES(?,?,?,?,?)");
				stmt.setLong(1, BATCH_ID);
				stmt.setTime(2, new Time(obj.getTime().getTime()));
				stmt.setInt(3, obj.getTransfer());
				stmt.setInt(4, obj.getBandwidth());
				stmt.setString(5, pair);
				rows += stmt.executeUpdate();
			}
		} catch (SQLException e) {e.printStackTrace();}
		finally {try {con.close();} catch (SQLException e) {e.printStackTrace();}}
		return rows;
	}
	public int batch_run(int rowsDevStats,int rowsControllerStats,int rowsPortStats, int rowsIperf3Total,int rowsPingTotal, int rowsCoeff, int blocked) {
		Connection con = getConnection();
		int rows = 0;
		try {
			PreparedStatement stmt = con.prepareStatement(
					"INSERT INTO batch_run VALUES(?,?,?,?,?,?,?,?,?)");
			stmt.setLong(1, BATCH_ID);
			stmt.setInt(2, rowsDevStats);
			stmt.setInt(3, rowsControllerStats);
			stmt.setInt(4, rowsPortStats);
			stmt.setInt(5, rowsIperf3Total);
			stmt.setInt(6, rowsPingTotal);
			stmt.setInt(7, rowsCoeff);
			stmt.setInt(8, Reader.getDropped()/2);
			stmt.setInt(9, blocked);
			rows = stmt.executeUpdate();
		} catch (SQLException e) {e.printStackTrace();}
		finally {try {con.close();} catch (SQLException e) {e.printStackTrace();}}
		return rows;
	}
	public int insertPingStats(ArrayList<RTT> rtt) {
		int rows = 0;
		Connection con = getConnection();
		Iterator<RTT> it = rtt.iterator();
		try {
			while(it.hasNext()) {
				RTT obj = it.next();
				PreparedStatement stmt = con.prepareStatement(
						"INSERT INTO pingStat VALUES(?,?,?,?,?)");
				stmt.setLong(1, BATCH_ID);
				stmt.setTime(2, new Time(obj.getTime().getTime()));
				stmt.setDouble(3, obj.getMin());
				stmt.setDouble(4, obj.getAvg());
				stmt.setDouble(5, obj.getMax());
				rows += stmt.executeUpdate();
			}
		} catch (SQLException e) {e.printStackTrace();}
		finally {try {con.close();} catch (SQLException e) {e.printStackTrace();}}
		return rows;
	}
	public int insertPingRep(ArrayList<Ping> pingRep, String pingPair) {
		int rows = 0;
		Connection con = getConnection();
		Iterator<Ping> it = pingRep.iterator();
		try {
			while(it.hasNext()) {
				Ping obj = it.next();
				PreparedStatement stmt = con.prepareStatement(
						"INSERT INTO pingRepStat VALUES(?,?,?,?)");
				stmt.setLong(1, BATCH_ID);
				stmt.setTime(2, new Time(obj.getTime().getTime()));
				stmt.setDouble(3, obj.getRtt());
				stmt.setString(4, pingPair);
				rows += stmt.executeUpdate();
			}
		} catch (SQLException e) {e.printStackTrace();}
		finally {try {con.close();} catch (SQLException e) {e.printStackTrace();}}
		return rows;
	}
	public int insert(String[] args) {
		int rows = 0;
		Connection con = getConnection();
		try {
			String state = "INSERT INTO "+args[1]+ " VALUES(" + args[2];
			for(int i=3;i<args.length;i++)
				state += ","+args[i];
			state += ")";
			System.out.println(state);
			PreparedStatement stmt = con.prepareStatement(state);
			rows += stmt.executeUpdate();
			
		} catch (SQLException e) {e.printStackTrace();}
		finally {try {con.close();} catch (SQLException e) {e.printStackTrace();}}
		return rows;
	}
	public void delete(String arg) {
		Connection con = getConnection();
		try {
			Statement stmt = con.createStatement();
			for (String table:tables)
				stmt.execute("DELETE FROM "+table+" WHERE BATCH_ID="+arg);
			System.out.println("Deleteted "+arg);
		} catch (SQLException e) {e.printStackTrace();}
		finally {try {con.close();} catch (SQLException e) {e.printStackTrace();}}
	}
	public void truncate() {
		Connection con = getConnection();
		try {
			Statement stmt = con.createStatement();
			for (String table:tables)
				stmt.execute("TRUNCATE TABLE "+table);
			System.out.println("Truncated all tables");
		} catch (SQLException e) {e.printStackTrace();}
		finally {try {con.close();} catch (SQLException e) {e.printStackTrace();}}
	}
	public void drop() {
		Connection con = getConnection();
		try {
			Statement stmt = con.createStatement();
			for (String table:tables)
				stmt.execute("DROP TABLE "+table);
			System.out.println("Dropped all tables");
		} catch (SQLException e) {e.printStackTrace();}
		finally {try {con.close();} catch (SQLException e) {e.printStackTrace();}}
	}
	public int insertCoeffiecients(ArrayList<Coefficient> coefs) {
		int rows = 0;
		Connection con = getConnection();
		Iterator<Coefficient> it = coefs.iterator();
		try {
			while(it.hasNext()) {
				Coefficient obj = it.next();
				PreparedStatement stmt = con.prepareStatement(
						"INSERT INTO coefficients VALUES(?,?,?)");
				stmt.setLong(1, BATCH_ID);
				stmt.setInt(2, obj.getSerialNumber());
				stmt.setDouble(3, obj.getCoefficient());
				rows += stmt.executeUpdate();
			}
		} catch (SQLException e) {e.printStackTrace();}
		finally {try {con.close();} catch (SQLException e) {e.printStackTrace();}}
		return rows;
	}
	public ArrayList<String> getBatchIDs(String string) {
		ArrayList<String> al = new ArrayList<>();
		Connection con = getConnection();
		try {
			PreparedStatement stmt = con.prepareStatement("SELECT BATCH_ID FROM sessionMap WHERE toUse like "+string);
			ResultSet rs = stmt.executeQuery();
			while(rs.next()) {
				al.add(rs.getString(1));
			}
		} catch (SQLException e) {e.printStackTrace();}
		finally {try {con.close();} catch (SQLException e) {e.printStackTrace();}}
		return al;
	}

}
