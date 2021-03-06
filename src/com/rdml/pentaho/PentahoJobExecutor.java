package com.rdml.pentaho;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.rdml.pentaho.db.PDIManager;
import com.rdml.pentaho.util.PentahoLogDevice;
import com.rdml.pentaho.util.PropertyLoader;
import com.rdml.pentaho.util.SysCommandExecutor;

/**
 * Idea here is to seperate pentaho job run into smaller time chunk, and it aims
 * to prevent the out of memory problem when parsing a huge amount of logs
 * 
 * The process will be keeping record of exeucted log�s hour, and scheduled to
 * be run each hour that will use new date and hour pass to the pentaho
 * MainLoaderJob which then will utilize the date and hour info to parse matched
 * logs
 * 
 * @author ck
 * 
 */
public class PentahoJobExecutor {

	private final static String LEVEL_DAY = "DAY";
	private final static String LEVEL_HOUR = "HOUR";
	private final static String MODE_AUTO = "AUTO";
	private final static String MODE_TODAY = "TODAY";
	private final static String MODE_YESTERDAY = "YESTERDAY";
	private final static String MODE_ALL = "ALL";

	/** args 0-9 are for Pentaho Kitchen, 10-12 are for JobExecutor run behavior
	 * args[0] kitchen.sh location
	 * args[1] repository 
	 * args[2] user
	 * args[3] password
	 * args[4] directory
	 * args[5] job
	 * args[6] maxloglines
	 * args[7] maxlogtimeout
	 * args[8] log level
	 * args[9] log file location
	 * args[10] DAY / HOUR to decide what level the log time will be generated
	 * args[11] AUTO / TODAY,YESTERDAY / ALL to decide the mode of log time generation whether its go to find from records or run for today or yesterday all the missing hours; ALL will show all the hours if specified date otherwise will use current date's hours until now \n
	 * args[12] (Optional) yyyy-MM-dd Give the date yyyy-MM-dd of which application will go to check in the record table, \n 
	 * 			if DAY is used and AUTO: generate LOG_TIME for that day yyyy_MM_dd\n
	 *			if DAY is used and TODAY, YESTERDAY: doesn�t matter whether given date is today or not, use today yyyy_MM_dd \n
	 *			if HOUR is used and AUTO: generate LOG_TIME for that day that missing or unsuccessful records, call the command for each identified hours that day yyyy_MM_dd_HH \n
	 *			if HOUR is used and TODAY, YESTERDAY: generaete LOG_TIME for today�s or yesterday�s missing records or unsuccessful records, call the command for each identified hours yyyy_MM_dd_HH \n
	 * args[13] (Optional, default PROD) MODE to decide whether its PROD or TEST, which will change the behavior of PDI loader, such as log file path, steps execution 
	 * args[14] (Optional, default /var/log/cdr) the log files root folder, under it should contain billinggate, download, wowza, xss, xdm log folders   			 
	 * @param args
	 */
	public static void main(String[] args) {

		if (args.length >= 12) {

			String logFilePath = args[9];
			String generateLevel = args[10];
			String generateMode = args[11];
			String specifyDateStr = null;
			String mode = null;
			String logRootPath = null;
			
			if (args.length >= 13) {
				specifyDateStr = args[12];
			}
			if (args.length >= 14) {
				mode = args[13];
			}
			if (args.length >= 15) {
				logRootPath = args[14];
			}

			StringBuilder commandBuilder = new StringBuilder();
			commandBuilder.append(args[0]).append(" ")
			.append("-rep:").append(args[1]).append(" ")
			.append("-user:").append(args[2]).append(" ")
			.append("-pass:").append(args[3]).append(" ")
			.append("-dir:").append(args[4]).append(" ")
			.append("-job:").append(args[5]).append(" ")
			.append("-maxloglines:").append(args[6]).append(" ")
			.append("-maxlogtimeout:").append(args[7]).append(" ")
			.append("-level:").append(args[8]).append(" ");
			//.append("-logfile:").append(logFilePath).append(" "); // will use >> for output redirection instead

			PDIManager pdiManager = null; 
			Map<Date, String> logTimeCommands = new LinkedHashMap<Date, String> ();


			DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
			DateFormat dayLogDateFormat = new SimpleDateFormat("yyyy_MM_dd");
			DateFormat hourLogDateFormat = new SimpleDateFormat("yyyy_MM_dd_HH");
			DateFormat yearDF = new SimpleDateFormat("yyyy");
			DateFormat monthDF = new SimpleDateFormat("MM");
			DateFormat dayDF = new SimpleDateFormat("dd");
			DateFormat hourDF = new SimpleDateFormat("HH");

			try {
				pdiManager = PDIManager.getInstance();
			} catch (Exception e) {
				System.out.println("Not able to create PDIManager " + e.getMessage());
				e.printStackTrace();
			}

			if (generateLevel.equalsIgnoreCase(LEVEL_HOUR)) {
				if (generateMode.equalsIgnoreCase(MODE_AUTO)) {
					if (null != specifyDateStr && "" != specifyDateStr) {
						// HOUR,AUTO, with given date : get that date�s missing records
						Date specifyDate;
						try {
							specifyDate = dateFormat.parse(specifyDateStr);
							Collection<Date> missingLogTimes = getMissingHOURLogTime(pdiManager, specifyDate);
							for (Date missingLogTime : missingLogTimes) {
								if (null != missingLogTime) {
									String logYear = yearDF.format(missingLogTime);
									String logMonth = monthDF.format(missingLogTime);
									String logDay = dayDF.format(missingLogTime);
									String logHour = hourDF.format(missingLogTime); 
									String commandLine = 
											commandBuilder.toString() + 
											" -param:LOG_YEAR=" + logYear +
											" -param:LOG_MONTH=" + logMonth +
											" -param:LOG_DAY=" + logDay +
											" -param:LOG_HOUR=" + logHour;
									logTimeCommands.put(missingLogTime, commandLine);
								}
							}
						} catch (ParseException e) {
							System.out.println("Error when generate log times and command for HOUR, AUTO, " + specifyDateStr);
							e.printStackTrace();
						}
					} else {
						// HOUR,AUTO : get the latest logtime of record, then plus one hour
						Date latestLogTime = null;
						latestLogTime = getHOURAUTOLogTime(pdiManager);

						String logYear = yearDF.format(latestLogTime);
						String logMonth = monthDF.format(latestLogTime);
						String logDay = dayDF.format(latestLogTime);
						String logHour = hourDF.format(latestLogTime); 
						String commandLine = 
								commandBuilder.toString() + 
								" -param:LOG_YEAR=" + logYear +
								" -param:LOG_MONTH=" + logMonth +
								" -param:LOG_DAY=" + logDay +
								" -param:LOG_HOUR=" + logHour;
						logTimeCommands.put(latestLogTime, commandLine);
					}
				} else if (generateMode.equalsIgnoreCase(MODE_TODAY) || generateMode.equalsIgnoreCase(MODE_YESTERDAY)) {
					//if (null != specifyDateStr && "" != specifyDateStr) {
					// HOUR,TODAY, with given date : get that today�s missing records, ignor givien specified date
					Date specifyDate = null;
					if (generateMode.equalsIgnoreCase(MODE_TODAY)) {
						specifyDate = new Date();
					} else if (generateMode.equalsIgnoreCase(MODE_YESTERDAY)) {
						specifyDate = getYesterday();
					}
					Collection<Date> missingLogTimes = getMissingHOURLogTime(pdiManager, specifyDate);
					for (Date missingLogTime : missingLogTimes) {
						//String commandLine = commandBuilder.toString() + "-param:LOG_TIME=" + hourLogDateFormat.format(missingLogTime);
						
						String logYear = yearDF.format(missingLogTime);
						String logMonth = monthDF.format(missingLogTime);
						String logDay = dayDF.format(missingLogTime);
						String logHour = hourDF.format(missingLogTime);
						String commandLine = 
								commandBuilder.toString() + 
								" -param:LOG_YEAR=" + logYear +
								" -param:LOG_MONTH=" + logMonth +
								" -param:LOG_DAY=" + logDay +
								" -param:LOG_HOUR=" + logHour;
						
						logTimeCommands.put(missingLogTime, commandLine);
					}

					/*} else {
						// HOUR,TODAY : get the today�s logtime of record, then plus one hour
						Date latestLogTime = null;
						String lastestLogTimeStr;
						latestLogTime = getHOURTODAYLogTime(pdiManager, new Date());
						lastestLogTimeStr = logDateFormat.format(latestLogTime);

						commandBuilder.append("-param:LOG_TIME=").append(lastestLogTimeStr);
						String commandLine = commandBuilder.toString();
						logTimeCommands.put(latestLogTime, commandLine);
					}*/
				} else if (generateMode.equalsIgnoreCase(MODE_ALL)) {
					if (null != specifyDateStr && "" != specifyDateStr) {
						// HOUR,ALL, with given date : get that date�s all hours
						Date specifyDate;
						try {
							specifyDate = dateFormat.parse(specifyDateStr);
							Collection<Date> allLogTimes = getAllHOURLogTime(pdiManager, specifyDate);
							for (Date logTime : allLogTimes) {
								if (null != logTime) {
									String logYear = yearDF.format(logTime);
									String logMonth = monthDF.format(logTime);
									String logDay = dayDF.format(logTime);
									String logHour = hourDF.format(logTime); 
									String commandLine = 
											commandBuilder.toString() + 
											" -param:LOG_YEAR=" + logYear +
											" -param:LOG_MONTH=" + logMonth +
											" -param:LOG_DAY=" + logDay +
											" -param:LOG_HOUR=" + logHour;
									logTimeCommands.put(logTime, commandLine);
								}
							}
						} catch (ParseException e) {
							System.out.println("Error when generate log times and command for HOUR, ALL, " + specifyDateStr);
							e.printStackTrace();
						}
					} else {
						// HOUR,ALL : get today's all past hours
						Date today = new Date();
						Collection<Date> allLogTimes = getAllHOURLogTime(pdiManager, today);
						for (Date logTime : allLogTimes) {
							if (null != logTime) {
								String logYear = yearDF.format(logTime);
								String logMonth = monthDF.format(logTime);
								String logDay = dayDF.format(logTime);
								String logHour = hourDF.format(logTime); 
								String commandLine = 
										commandBuilder.toString() + 
										" -param:LOG_YEAR=" + logYear +
										" -param:LOG_MONTH=" + logMonth +
										" -param:LOG_DAY=" + logDay +
										" -param:LOG_HOUR=" + logHour;
								logTimeCommands.put(logTime, commandLine);
							}
						}
					}
				}
			} else if (generateLevel.equalsIgnoreCase(LEVEL_DAY)) {
				if (generateMode.equalsIgnoreCase(MODE_AUTO)) {
					if (null != specifyDateStr && "" != specifyDateStr) {
						// use specified date
						try {
							Date specifyDate = dateFormat.parse(specifyDateStr);
							//String commandLine = commandBuilder.toString() + "-param:LOG_TIME=" + dayLogDateFormat.format(specifyDate);
							String logYear = yearDF.format(specifyDate);
							String logMonth = monthDF.format(specifyDate);
							String logDay = dayDF.format(specifyDate);
							
							String commandLine = 
									commandBuilder.toString() + 
									" -param:LOG_YEAR=" + logYear +
									" -param:LOG_MONTH=" + logMonth +
									" -param:LOG_DAY=" + logDay +
									" -param:LOG_HOUR=";
							
							logTimeCommands.put(specifyDate, commandLine);
						} catch (ParseException e) {
							System.out.println("Error when generate log times and command for DAY, AUTO, " + specifyDateStr);
							e.printStackTrace();
						}
					} else {
						// has record ? get latest log date : ask input log date in format yyyy-MM-dd 	-------> yyyy_MM_dd
						Date logDate = getDAYAUTOLogTime(pdiManager);
						//String commandLine = commandBuilder.toString() + "-param:LOG_TIME=" + dayLogDateFormat.format(logDate);
						String logYear = yearDF.format(logDate);
						String logMonth = monthDF.format(logDate);
						String logDay = dayDF.format(logDate);
						
						String commandLine = 
								commandBuilder.toString() + 
								" -param:LOG_YEAR=" + logYear +
								" -param:LOG_MONTH=" + logMonth +
								" -param:LOG_DAY=" + logDay +
								" -param:LOG_HOUR=";
						
						logTimeCommands.put(logDate, commandLine);
					}
				} else if (generateMode.equalsIgnoreCase(MODE_ALL) || generateMode.equalsIgnoreCase(MODE_TODAY) || generateMode.equalsIgnoreCase(MODE_YESTERDAY)) {
					// use today�s date
					Date logDate = null;
					if (generateMode.equalsIgnoreCase(MODE_ALL) || generateMode.equalsIgnoreCase(MODE_TODAY)) {
						logDate = getDAYTODAYLogTime(pdiManager, new Date());
					} else if (generateMode.equalsIgnoreCase(MODE_YESTERDAY)) {
						logDate = getYesterday();
					}
					//String commandLine = commandBuilder.toString() + "-param:LOG_TIME=" + dayLogDateFormat.format(logDate);
					String logYear = yearDF.format(logDate);
					String logMonth = monthDF.format(logDate);
					String logDay = dayDF.format(logDate);
					
					String commandLine = 
							commandBuilder.toString() + 
							" -param:LOG_YEAR=" + logYear +
							" -param:LOG_MONTH=" + logMonth +
							" -param:LOG_DAY=" + logDay +
							" -param:LOG_HOUR=";
					
					logTimeCommands.put(logDate, commandLine);
				}
			}



			SysCommandExecutor cmdExecutor = new SysCommandExecutor();
			/*cmdExecutor.setOutputLogDevice(new LogDevice());
			cmdExecutor.setErrorLogDevice(new LogDevice());*/
			BufferedWriter out = null;
			try {
				out = attachLogDevice(cmdExecutor, logFilePath);
			} catch (Exception e) {
				System.out.println("Not able to assign LogDevice for CommandExecutor");
			}

			for (Date logTime : logTimeCommands.keySet()) {
				String commandLine = logTimeCommands.get(logTime);
				Date startTime = null;
				Date endTime = null;
				int exitStatus = 0;
				String success = "succeeded";
				try {
					startTime = new Date();
					//commandLine = commandLine + " > " + logFilePath;
					if (null != mode) {
						commandLine = commandLine + " -param:MODE=" + mode;
					}
					if (null != logRootPath) {
						commandLine = commandLine + " -param:LOG_ROOT_FOLDER=" + logRootPath;
					}
					System.out.println(commandLine);
					
					exitStatus = cmdExecutor.runCommand(commandLine);
					endTime = new Date();
					if (exitStatus != 0) {
						success = "failure";
					}
				} catch (Exception e) {
					success = "failure";
					System.err.println("Job execution has problem : "
							+ e.getMessage());
				}

				if (null != pdiManager && null != logTime && null != startTime && null != endTime) {
					boolean insertResult = pdiManager.insertNewExecuteReocrd(logTime, startTime, endTime, commandLine, success, generateLevel, generateMode, specifyDateStr);
					if (!insertResult) {
						System.out.println("Not able to record result on " + startTime + " with command: " + commandLine);
					} else {
						System.out.println("Job Execution finnished with exit code: "
								+ exitStatus + " and recorded in DB JobExecutorRecord Table");
					}
				}
			}
			// close the writer 
			if (null != out) {
				try {
					out.close();
				} catch (IOException e) {
					System.out.println("Not able to close LogDevice stream");
				}
				out = null;
			}

			try {
				String destLogPath = moveLogFile(logFilePath);
				System.out.println("Log " + logFilePath + " has been copied to " + destLogPath);
			} catch (Exception e) {
				System.out.println("Not able to move the kitchen log file " + logFilePath + " " + e.getMessage());
			}
			
		} else {
			System.out.println("Arguments number not correct, need at least 12 arguments, \nargs 0-9 are for Pentaho Kitchen, 10-12 are for JobExecutor run behavior : ");
			System.out.println("args[0] kitchen.sh location");
			System.out.println("args[1] repository"); 
			System.out.println("args[2] user");
			System.out.println("args[3] password");
			System.out.println("args[4] directory");
			System.out.println("args[5] job");
			System.out.println("args[6] maxloglines");
			System.out.println("args[7] maxlogtimeout");
			System.out.println("args[8] log level");
			System.out.println("args[10] DAY / HOUR to decide what level the log time will be generated");
			System.out.println("args[9] log file location");
			System.out.println("args[11] AUTO / TODAY,YESTERDAY / ALL to decide the mode of log time generation whether its go to find from records or run for today or yesterday all the missing hours; ALL will show all the hours if specified date otherwise will use current date's hours until now \n");
			System.out.println("args[12] (Optional) Give the date yyyy-MM-dd of which application will go to check in the record table, \n" 
					+ "\tif DAY is used and AUTO: generate LOG_TIME for that day yyyy_MM_dd\n"
					+ "\tif DAY is used and TODAY, YESTERDAY: doesn�t matter whether given date is today/yesterday or not, use today yyyy_MM_dd \n"
					+ "\tif HOUR is used and AUTO: generate LOG_TIME for that day that missing or unsuccessful records, call the command for each identified hours that day yyyy_MM_dd_HH \n"
					+ "\tif HOUR is used and TODAY, YESTERDAY: generaete LOG_TIME for today�s/yesterday�s missing records or unsuccessful records, call the command for each identified hours yyyy_MM_dd_HH");
			System.out.println("args[13] (Optional, default PROD) MODE to decide whether its PROD or TEST, which will change the behavior of PDI loader, such as log file path, steps execution");
			System.out.println("args[14] (Optional, default /var/log/cdr) the log files root folder, under it should contain billinggate, download, wowza, xss, xdm log folders");
		}
	}


	/**
	 * HOUR,AUTO : get the latest logtime of record, then plus one hour
	 * @param pdiManager
	 * @return
	 */
	private static Date getHOURAUTOLogTime(PDIManager pdiManager) {
		Date latestLogTime = null;
		try {
			latestLogTime = pdiManager.getLatestLogTime();
			if (null != latestLogTime) {
				Calendar cal = Calendar.getInstance();
				cal.setTime(latestLogTime);
				cal.add(Calendar.HOUR_OF_DAY, 1);
				latestLogTime = cal.getTime();
			} else {
				DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH");
				BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
				System.out.print("There was no previous log date record exist, because suggest one log time (yyyy-MM-dd HH): ");
				String startLogTime;
				try {
					startLogTime = br.readLine();
					latestLogTime = df.parse(startLogTime);

					br.close();
				} catch (Exception e) {
					System.out
					.println("Not able to read log time: " + e.getMessage());
				}
			}
			//System.out.println(lastestLogTimeStr);
		} catch (Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
		return latestLogTime;
	}

	/**
	 * Get HOUR, TODAY with/without specified date
	 * @param pdiManager
	 * @return
	 */
	private static Date getHOURTODAYLogTime(PDIManager pdiManager, Date today) {
		Date logTime = pdiManager.getLatestLogTimeWithDate(today);
		if (null == logTime) {
			// Truncate the date�s hour minute second millisecond
			Calendar cal = Calendar.getInstance();
			cal.setTime(today);
			cal.set(Calendar.HOUR, 0);
			cal.set(Calendar.MINUTE, 0);
			cal.set(Calendar.SECOND, 0);
			cal.set(Calendar.MILLISECOND, 0);
			return cal.getTime();
		} else {
			// Plus one hour in the latest record log time
			Calendar cal = Calendar.getInstance();
			cal.setTime(logTime);
			cal.add(Calendar.HOUR, 1);
			return cal.getTime();
		}
	}

	/**
	 * HOUR,AUTO, with given date : get that date�s missing records
	 * @param pdiManager
	 * @param specifiedDate
	 * @return
	 */
	private static Collection<Date> getMissingHOURLogTime(PDIManager pdiManager, Date specifiedDate) {
		return pdiManager.getMissingLogHoursByDate(specifiedDate);

	}
	
	private static Collection<Date> getAllHOURLogTime(PDIManager pdiManager, Date date) {
		if (date == null) return null;

		DateFormat df1 = new SimpleDateFormat("yyyy-MM-dd");
		DateFormat df2 = new SimpleDateFormat("HH");

		// 1. Generate all Hours for that date
		List<java.util.Date> allHours = new LinkedList<java.util.Date> ();
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
			for (int hour = 0; hour < currentHour; hour ++) {
				cal.set(Calendar.HOUR_OF_DAY, hour);
				allHours.add(cal.getTime());
			}
		} else {
			// date is not today so then need 0-23 hours
			for (int hour = 0; hour < 24; hour ++ ) {
				cal.set(Calendar.HOUR_OF_DAY, hour);
				allHours.add(hour, cal.getTime());
			}
		}
		
		return allHours;
	}

	/**
	 * DAY,AUTO : get the latest logtime of record
	 * @param pdiManager
	 * @return
	 */
	private static Date getDAYAUTOLogTime(PDIManager pdiManager) {
		Date latestLogTime = null;
		try {
			latestLogTime = pdiManager.getLatestLogTime();
			if (null == latestLogTime) {
				DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
				BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
				System.out.print("There was no previous log date record exist, because suggest one log date (yyyy-MM-dd): ");
				String startLogTime;
				try {
					startLogTime = br.readLine();
					latestLogTime = df.parse(startLogTime);

					br.close();
				} catch (Exception e) {
					System.out
					.println("Not able to read log time: " + e.getMessage());
				}
			}

		} catch (Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
		return latestLogTime;
	}

	/**
	 * Get DAY, TODAY with/without specified date
	 * @param pdiManager
	 * @return
	 */
	private static Date getDAYTODAYLogTime(PDIManager pdiManager, Date today) {
		// Truncate the date�s hour minute second millisecond
		Calendar cal = Calendar.getInstance();
		cal.setTime(today);
		cal.set(Calendar.HOUR, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
		return cal.getTime();

	}

	/**
	 * Get DAY, YESTERDAY with/without specified date
	 * @return
	 */
	private static Date getYesterday() {
		// Truncate the date�s hour minute second millisecond
		Calendar cal = Calendar.getInstance();
		cal.setTime(new Date());
		cal.set(Calendar.HOUR, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
		cal.add(Calendar.DATE, -1);
		return cal.getTime();

	}
	
	private static String moveLogFile(String sourceLogPath)  throws Exception{
		String kitchenLogFolder = PropertyLoader.getString("kitchen.log.folder");
		File source = new File(sourceLogPath);
		String fileName = source.getName();
		
		Date timestamp = new Date();
		DateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
		File dest = new File(kitchenLogFolder + fileName + "_" + df.format(timestamp));
		
		InputStream in = new FileInputStream(source);
		OutputStream out = new FileOutputStream(dest);
		
		byte[] buf = new byte[1024];
		int len;
		while((len = in.read(buf)) > 0) {
			out.write(buf, 0, len);
		}
		
		in.close();
		out.close();
		
		source.deleteOnExit();
		
		return dest.getAbsolutePath();
	}
	
	
	private static BufferedWriter attachLogDevice(SysCommandExecutor cmdExecutor, String logFilePath) throws Exception {
		FileWriter fw = new FileWriter(new File(logFilePath), true);
		BufferedWriter out = new BufferedWriter(fw);
		
		cmdExecutor.setOutputLogDevice(new PentahoLogDevice(logFilePath, out));
		cmdExecutor.setErrorLogDevice(new PentahoLogDevice(logFilePath, out));
		
		return out;
	}
	
	
}
