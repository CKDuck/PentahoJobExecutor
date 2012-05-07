package com.rdml.pentaho.db;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.rdml.pentaho.util.PropertyLoader;



public class PDIManager extends DatabaseManager {
	private static PDIManager instance;


	private PDIManager(String driver, String url, String user, String password) throws ClassNotFoundException {
		super(driver, url, user, password);
	}

	public java.util.Date getLatestLogTime() {
		String query = 
				"SELECT log_time " +
				"FROM JobExecutorRecord " +
				"WHERE success LIKE 'succeeded' " +
				"AND level LIKE 'HOUR' " +
				"ORDER BY log_time DESC LIMIT 1";
		Timestamp logTime = null;
		Connection connection = null;
		Statement s = null;
		try {
			connection = getConnection();
			s = getStatement(connection);
			ResultSet rs = s.executeQuery(query);
			while (rs.next()) {
				logTime = rs.getTimestamp("log_time");
			}
		} catch (Exception e) {
			System.out.println("Database connection not able to be established: " + e.getMessage());
			e.printStackTrace();
		} finally {
			try {
				closeStatement(s);
			} catch (Exception e) {
				System.out.println("Not able to close statement : " + e.getMessage());
			}
			releaseConn();
		}
		if (logTime != null) {
			return new java.util.Date(logTime.getTime());
		} else {
			return null;
		}
	}

	public java.util.Date getLatestLogTimeWithDate(java.util.Date specifiedDate) {
		if (null ==  specifiedDate) return null;
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		String query = 
				"SELECT log_time " +
						"FROM JobExecutorRecord " +
						"WHERE success LIKE 'succeeded' " +
						"AND LEFT(log_time, 10) = '" + df.format(specifiedDate) + "' " +
						"AND level LIKE 'HOUR' " +
						"ORDER BY log_time DESC LIMIT 1";
		Timestamp logTime = null;
		Connection connection = null;
		Statement s = null;
		try {
			connection = getConnection();
			s = getStatement(connection);
			ResultSet rs = s.executeQuery(query);
			while (rs.next()) {
				logTime = rs.getTimestamp("log_time");
			}
		} catch (Exception e) {
			System.out.println("Database connection not able to be established: " + e.getMessage());
			e.printStackTrace();
		} finally {
			try {
				closeStatement(s);
			} catch (Exception e) {
				System.out.println("Not able to close statement : " + e.getMessage());
			}
			releaseConn();
		}
		if (logTime != null) {
			return new java.util.Date(logTime.getTime());
		} else {
			return null;
		}
	}

	public Collection<java.util.Date> getMissingLogHoursByDate(java.util.Date date) {
		if (date == null) return null;

		DateFormat df1 = new SimpleDateFormat("yyyy-MM-dd");
		DateFormat df2 = new SimpleDateFormat("HH");

		// 1. Generate all Hours for that date
		Map<Integer, java.util.Date> allHours = new LinkedHashMap<Integer, java.util.Date> ();
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);

		java.util.Date today = new java.util.Date();
		String dayOfDate = df1.format(date);
		String todayDate = df1.format(today);
		if (dayOfDate.equalsIgnoreCase(todayDate)) {
			// If date is today then only the hour that have passed will be counted 
			int currentHour = Integer.parseInt(df2.format(today));
			for (int hour = 0; hour <= currentHour; hour ++) {
				cal.set(Calendar.HOUR_OF_DAY, hour);
				allHours.put(hour, cal.getTime());
			}
		} else {
			// date is not today so then need 0-23 hours
			for (int hour = 0; hour < 24; hour ++ ) {
				cal.set(Calendar.HOUR_OF_DAY, hour);
				allHours.put(hour, cal.getTime());
			}
		}


		String query = 
				"SELECT log_time " +
						"FROM JobExecutorRecord " +
						"WHERE success LIKE 'succeeded' " +
						"AND LEFT(log_time, 10) = '" + df1.format(date) + "' " +
						"AND level LIKE 'HOUR' " +
						"ORDER BY log_time ASC";

		Connection connection = null;
		Statement s = null;

		Map<Integer, java.util.Date> successHours = new HashMap<Integer, java.util.Date> ();
		try {
			connection = getConnection();
			s = getStatement(connection);
			ResultSet rs = s.executeQuery(query);

			java.util.Date logTime = null;
			while (rs.next()) {
				logTime = new java.util.Date(rs.getTimestamp("log_time").getTime());
				successHours.put(Integer.parseInt(df2.format(logTime)), logTime);
			}
		} catch (Exception e) {
			System.out.println("Database connection not able to be established: " + e.getMessage());
			e.printStackTrace();
		} finally {
			try {
				closeStatement(s);
			} catch (Exception e) {
				System.out.println("Not able to close statement : " + e.getMessage());
			}
			releaseConn();
		}
		if (successHours.size() > 0) {
			// remove successful hours
			List<java.util.Date> retVal = new LinkedList<java.util.Date> ();
			
			for (Integer hour : allHours.keySet()) {
				boolean success = false;
				for (Integer successHour : successHours.keySet()) {
					if (successHour == hour) {
						success = true;
						break;
					}
				}

				if (!success) {
					retVal.add(allHours.get(hour));
				}

			}
			//TODO: Make order of return dates
			Collections.sort(retVal, new Comparator<java.util.Date> () {

				@Override
				public int compare(Date o1, Date o2) {
					return o1.compareTo(o2);
				}
				
				
			});
			return retVal;
		} else {
			return allHours.values();
		}

	}


	public boolean insertNewExecuteReocrd(java.util.Date logTime, java.util.Date startTime, java.util.Date endTime, String kitchenCommand, String success, String level, String mode, String specifiedDateStr) {
		boolean result = false;
		java.util.Date recordTime = new java.util.Date();
		double secSpend = endTime.getTime() - startTime.getTime();

		String sql = "INSERT INTO JobExecutorRecord(RECORD_TIME, LOG_TIME, START_TIME, END_TIME, KITCHEN_COMMAND, TIME_SPEND, SUCCESS, LEVEL, MODE, SPECIFIEDDATE) " +
				"VALUES(?, ?, ?, ?, ?, SEC_TO_TIME(?), ?, ?, ?, ?)";
		Connection c = null;
		PreparedStatement ps = null;
		try {
			c = getConnection();
			ps = getPreparedStatement(c, sql);
			ps.setTimestamp(1, new Timestamp(recordTime.getTime()));
			ps.setTimestamp(2, new Timestamp(logTime.getTime()));
			ps.setTimestamp(3, new Timestamp(startTime.getTime()));
			ps.setTimestamp(4, new Timestamp(endTime.getTime()));
			ps.setString(5, kitchenCommand);
			ps.setDouble(6, secSpend / 1000);
			ps.setString(7, success);
			ps.setString(8, level);
			ps.setString(9, mode);
			ps.setString(10, specifiedDateStr);
			result = ps.execute();
		} catch (Exception e) {
			System.err.println("Database connection not able to be established: " + e.getMessage());
			e.printStackTrace();
		} finally {
			try {
				closePreparedStatement(ps);
			} catch (Exception e) {
				System.err.println("Not able to close prepared statement : " + e.getMessage());
			}
			releaseConn();
		}

		return result;
	}


	public static PDIManager getInstance() throws ClassNotFoundException {
		if (instance == null) {
			instance = new PDIManager(
					PropertyLoader.getString("jdbc.driver"),
					PropertyLoader.getString("jdbc.url"),
					PropertyLoader.getString("jdbc.user"),
					PropertyLoader.getString("jdbc.password")
					);
		}
		return instance;
	}


	public void releaseConn(){
		try {
			releaseConnection(getConnection());
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}

