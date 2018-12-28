package com.batch.master;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import com.batch.util.Configuration;
import com.batch.util.CronExpression;
import com.batch.util.JobInfo;
import com.batch.util.LoggerUtil;
import com.batch.util.StrVal;

public class LoadInfo implements Runnable{

	Configuration conf = null;
	Map shareM = null;
	Logger log = null;
	String cfg = "";
	final static String clss = "loadinfo";
	
	Map jobconfMaping = null;
	Map jobTriggerMaping = null;
	Map jobStreamMapping = null;
	Map jobDependencyMaping = null;
	Map jobStepMaping = null;
	Map jobTimewindowMaping = null;
    public void initialize(String node,String cfgfile,Map shareM){
    	//init conf
    	this.cfg = cfgfile;
		this.conf = new Configuration();
		conf.initialize(new File(cfg));
		DefaultConf.initialize(conf);
		
    	this.shareM = shareM;
    	this.jobconfMaping = (Map) this.shareM.get(StrVal.MAP_KEY_JOB);
    	this.jobTriggerMaping = (Map) this.shareM.get(StrVal.MAP_KEY_TRIGGER);
    	this.jobStepMaping = (Map) this.shareM.get(StrVal.MAP_KEY_STEP);
    	this.jobTimewindowMaping = (Map) this.shareM.get(StrVal.MAP_KEY_TIMEWINDOW);
	}
	@Override
	public void run() {
		long starttime = System.currentTimeMillis();
		boolean flag = true;
		while(true){
			//init log
			SimpleDateFormat time = new SimpleDateFormat("yyyyMMdd");
			SimpleDateFormat timestamp = new SimpleDateFormat("yyyyMMddHHmmss");
			
			Date nowTime = new Date();
			String yyyymmdd = time.format(nowTime);
			log = new LoggerUtil().getLoggerByName(conf.get("sys.log.path"), clss+"_"+yyyymmdd);
			//refresh config
			long endtime = System.currentTimeMillis();
			long subval = endtime - starttime;
			long subvalconf = Long.parseLong(conf.get(StrVal.CONF_REF_TIME));
			//init conf
			if(subvalconf<=subval){
				log.info(clss+" refresh config.");
				conf.initialize(new File(cfg));
				DefaultConf.initialize(conf);
				starttime = endtime;
				flag = true;
			}
			if(flag){
				flag = false;
				if(conf.get(StrVal.JOB_CONF_TYPE).equals("file")){
					confFromFile(conf.get(StrVal.JOB_FILE_PATH));
				}else if(conf.get(StrVal.JOB_CONF_TYPE).equals("database")){
					log.info("conf from database.");
					Connection conn = JobInfo.getConnectionDB(conf);
					//job info
					confFromDB(conn,conf.get(StrVal.SQL_SHD_JOB_INFO));
					//trigger
					triggerFromDB(conn,conf.get(StrVal.SQL_SHD_JOB_TRIGGER));
					//step
					stepFromDB(conn,conf.get(StrVal.SQL_SHD_JOB_STEP));
					try {
						conn.close();
					} catch (SQLException e) {
						e.printStackTrace();
					}
				}
			}
			
			//log.info("*********************** Sleep ["+conf.get(StrVal.SYS_HEARTBEADT_TIME)+"] ***********************.");
			try {
				Thread.sleep(Long.parseLong(conf.get(StrVal.SYS_HEARTBEADT_TIME)));
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}


	public void confFromFile(String filenm){
		String result = null;
		FileReader fileReader = null;
		BufferedReader bufferedReader = null;
		if(conf.get(StrVal.SYS_DEBUG_FLAG).equals(conf.get(StrVal.SYSTEM_SUCCESS_FLAG))){
			log.info("load job info from file "+filenm+".");
		}
		File file = new File(filenm);
		if(file.exists()&&file.isFile()){
	      try {	
			 String read = null;
			 fileReader = new FileReader(file);
			 bufferedReader = new BufferedReader(fileReader);
			 while((read=bufferedReader.readLine())!=null){
				if(read.trim().length()==0){
					continue;
				}
				if(read.trim().startsWith("#")){
					continue;
				}
				String[] keyval = read.split(conf.get(StrVal.JOB_FILE_DELIMITER));
				String pool = keyval[0].trim().toUpperCase();
				String system = keyval[1].trim().toUpperCase();
				String job = keyval[2].trim().toUpperCase();
				String nodeName = keyval[3].trim();
				String jobType = keyval[5].trim().toUpperCase();
				String checkTimewindow = keyval[6].trim().toUpperCase();
				String checkTimeTrigger = keyval[7].trim().toUpperCase();
				String checkCalendar = keyval[8].trim().toUpperCase();
				String checkLastStatus = keyval[9].trim().toUpperCase();
				String priority = keyval[10];
				String enable = keyval[11].trim();
				
				Map tmpM = new HashMap();
				tmpM.put(conf.get(StrVal.JOB_ATTR_POOL), pool);
				tmpM.put(conf.get(StrVal.JOB_ATTR_SYSTEM), system);
				tmpM.put(conf.get(StrVal.JOB_ATTR_JOB), job);
				tmpM.put(conf.get(StrVal.JOB_ATTR_NODE), nodeName);
				tmpM.put(conf.get(StrVal.JOB_ATTR_JOBTYPE), jobType);
				tmpM.put(conf.get(StrVal.JOB_ATTR_CHECKTIMEWINDOW), checkTimewindow);
				tmpM.put(conf.get(StrVal.JOB_ATTR_CHECKTIMETRIGGER), checkTimeTrigger);
				tmpM.put(conf.get(StrVal.JOB_ATTR_CHECKCALENDAR), checkCalendar);
				tmpM.put(conf.get(StrVal.JOB_ATTR_CHECKLASTSTATUS), checkLastStatus);
				tmpM.put(conf.get(StrVal.JOB_ATTR_PRIORITY), priority);
				tmpM.put(conf.get(StrVal.JOB_ATTR_ENABLE), enable);
				String ctl = pool+"_"+system+"_"+job;
				jobconfMaping.put(ctl, tmpM);
			}
		  } catch (FileNotFoundException e) {
			e.printStackTrace();
		  }catch (IOException e) {
			e.printStackTrace();
		  }finally{
			try {
				bufferedReader.close();
				fileReader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		  }
		}else{
		   log.info(filenm +" conf is not exist.");
		}
	}
	
	public void confFromDB(Connection conn,String sqlText){
		Statement st = null;
        ResultSet rt = null;
        int index = 0;
   		try {
   			st = conn.createStatement();
   			rt = st.executeQuery(sqlText);
   			while(rt.next()) {
   				String pool = rt.getString(1).trim().toUpperCase();
				String system = rt.getString(2).trim().toUpperCase();
				String job = rt.getString(3).trim().toUpperCase();
				String nodeName = rt.getString(4).trim();
				String jobType = rt.getString(6).trim().toUpperCase();
				String checkTimewindow = rt.getString(7).trim().toUpperCase();
				String checkTimeTrigger = rt.getString(8).trim().toUpperCase();
				String checkCalendar = rt.getString(9).trim().toUpperCase();
				String checkLastStatus = rt.getString(10).trim().toUpperCase();
				String priority = rt.getString(11);
				String enable = rt.getString(12).trim();
				
				Map tmpM = new HashMap();
				tmpM.put(conf.get(StrVal.JOB_ATTR_POOL), pool);
				tmpM.put(conf.get(StrVal.JOB_ATTR_SYSTEM), system);
				tmpM.put(conf.get(StrVal.JOB_ATTR_JOB), job);
				tmpM.put(conf.get(StrVal.JOB_ATTR_NODE), nodeName);
				tmpM.put(conf.get(StrVal.JOB_ATTR_JOBTYPE), jobType);
				tmpM.put(conf.get(StrVal.JOB_ATTR_CHECKTIMEWINDOW), checkTimewindow);
				tmpM.put(conf.get(StrVal.JOB_ATTR_CHECKTIMETRIGGER), checkTimeTrigger);
				tmpM.put(conf.get(StrVal.JOB_ATTR_CHECKCALENDAR), checkCalendar);
				tmpM.put(conf.get(StrVal.JOB_ATTR_CHECKLASTSTATUS), checkLastStatus);
				tmpM.put(conf.get(StrVal.JOB_ATTR_PRIORITY), priority);
				tmpM.put(conf.get(StrVal.JOB_ATTR_ENABLE), enable);
				String ctl = pool+"_"+system+"_"+job;
				jobconfMaping.put(ctl, tmpM);
   			}
   		} catch (SQLException e) {
   			e.printStackTrace();
   		}
	}
	
	public void triggerFromDB(Connection conn,String sqlText){
		Statement st = null;
        ResultSet rt = null;
        int index = 0;
   		try {
   			st = conn.createStatement();
   			rt = st.executeQuery(sqlText);
   			while(rt.next()) {
   				String pool = rt.getString(1).trim().toUpperCase();
				String system = rt.getString(2).trim().toUpperCase();
				String job = rt.getString(3).trim().toUpperCase();
				String batchNum = rt.getString(5).trim().toUpperCase();
				String triggerType = rt.getString(6).trim();
				String expression = rt.getString(7).trim();
				String enable = rt.getString(8).trim().toUpperCase();
				
				Map tmpM = new HashMap();
				tmpM.put(conf.get(StrVal.JOB_ATTR_POOL), pool);
				tmpM.put(conf.get(StrVal.JOB_ATTR_SYSTEM), system);
				tmpM.put(conf.get(StrVal.JOB_ATTR_JOB), job);
				tmpM.put(conf.get(StrVal.JOB_ATTR_BATCHNUM), batchNum);
				tmpM.put(conf.get(StrVal.JOB_ATTR_TRIGGER_TYPE), triggerType);
				tmpM.put(conf.get(StrVal.JOB_ATTR_EXPRESSION), expression);
				tmpM.put(conf.get(StrVal.JOB_ATTR_ENABLE), enable);
				String ctl = batchNum+conf.get(StrVal.SYSTEM_CONTROL_DEL)+pool+"_"+system+"_"+job;
				jobTriggerMaping.put(ctl, tmpM);
   			}
   		} catch (SQLException e) {
   			e.printStackTrace();
   		}
	}
	
	public void stepFromDB(Connection conn,String sqlText){
		Statement st = null;
        ResultSet rt = null;
        int index = 0;
   		try {
   			st = conn.createStatement();
   			rt = st.executeQuery(sqlText);
   			while(rt.next()) {
   				String pool = rt.getString(1).trim().toUpperCase();
				String system = rt.getString(2).trim().toUpperCase();
				String job = rt.getString(3).trim().toUpperCase();
				String cmd = rt.getString(5).trim();
				String enable = rt.getString(6).trim().toUpperCase();
				
				Map tmpM = new HashMap();
				tmpM.put(conf.get(StrVal.JOB_ATTR_POOL), pool);
				tmpM.put(conf.get(StrVal.JOB_ATTR_SYSTEM), system);
				tmpM.put(conf.get(StrVal.JOB_ATTR_JOB), job);
				tmpM.put(conf.get(StrVal.JOB_ATTR_CMD), cmd);
				tmpM.put(conf.get(StrVal.JOB_ATTR_ENABLE), enable);
				String ctl = pool+"_"+system+"_"+job;
				jobStepMaping.put(ctl, tmpM);
   			}
   		} catch (SQLException e) {
   			e.printStackTrace();
   		}
	}
}
