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

public class TimeTiggerJob implements Runnable{

	Configuration conf = null;
	Map shareM = null;
	Map jobstatusM = null;
    Map jobsteamM = null;
	Logger log = null;
	String cfg = "";
	final static String clss = "trigger";
	Map triggerJobMap = new HashMap();
	Map jobconfMaping = null;
	Map jobTriggerMaping = null;
	CronExpression cronEx = null;
    public void initialize(String node,String cfgfile,Map shareM){
    	//init conf
    	this.cfg = cfgfile;
		this.conf = new Configuration();
		conf.initialize(new File(cfg));
		DefaultConf.initialize(conf);
		
    	this.shareM = shareM;
    	this.jobstatusM = (Map) this.shareM.get(StrVal.MAP_KEY_STATUS);
    	this.jobconfMaping = (Map) this.shareM.get(StrVal.MAP_KEY_JOB);
    	this.jobTriggerMaping = (Map) this.shareM.get(StrVal.MAP_KEY_TRIGGER);
	}
	@Override
	public void run() {
		long starttime = System.currentTimeMillis();
		boolean flg = true;	
		while(true){
			//init log
			SimpleDateFormat time = new SimpleDateFormat("yyyyMMdd");
			SimpleDateFormat timestamp = new SimpleDateFormat("yyyyMMddHHmmss");
			
			Date nowTime = new Date();
			String yyyymmdd = time.format(nowTime);
			log = new LoggerUtil().getLoggerByName(conf.get("sys.log.path"), clss+"_"+yyyymmdd);
			//init conf
			long endtime = System.currentTimeMillis();
			long subval = endtime - starttime;
			long subvalconf = Long.parseLong(conf.get(StrVal.CONF_REF_TIME));
			
			//init conf
			if(subvalconf<=subval){
				log.info(clss+" refresh config.");
				conf.initialize(new File(cfg));
				DefaultConf.initialize(conf);
				starttime = endtime;
				flg = true;
			}
			
			Map hisTriggerM = logFromFile(conf.get(StrVal.TRIGGER_JOB_LOG_PATH)+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.TRIGGER_JOB_LOG_NAME)+"_"+yyyymmdd+".log");
			
			Iterator iter = jobTriggerMaping.entrySet().iterator();
			while(iter.hasNext()){
				Entry entry = (Entry) iter.next();
				String key = (String) entry.getKey();
				Map val = (Map) entry.getValue();
				String triggertype = (String) val.get(conf.get(StrVal.JOB_ATTR_TRIGGER_TYPE));
				String triggerexpr = (String) val.get(conf.get(StrVal.JOB_ATTR_EXPRESSION));
				if(conf.get(StrVal.SYS_DEBUG_FLAG).equals(conf.get(StrVal.SYSTEM_SUCCESS_FLAG))){
					log.info(key+" trigger type["+triggertype+"].expression["+triggerexpr+"].");
				}
				if(triggertype.trim().toUpperCase().equals(StrVal.VAL_CONSTANT_CRON)){
					Date ntDt = null;
					boolean iss = false;
					Date d = null;
					String nextTriggertime = "";
					try {
						cronEx = new CronExpression(triggerexpr);
						d = new Date();
						ntDt = cronEx.getTimeAfter(d);
						iss = cronEx.isSatisfiedBy(d);
						nextTriggertime =timestamp.format(ntDt);
					} catch (ParseException e1) {
						e1.printStackTrace();
					}
					if(iss){
						String flag = StrVal.VAL_CONSTANT_N;
						String initNextTriggertime = "30001231235959";
						if(triggerJobMap.containsKey(key)){
							Map tmpMap = (Map) triggerJobMap.get(key);
							flag = (String) tmpMap.get(StrVal.MAP_KEY_FLAG);
						}else{
							Map tmpMap = new HashMap();
							tmpMap.put(StrVal.MAP_KEY_FLAG, StrVal.VAL_CONSTANT_Y);
							triggerJobMap.put(key, tmpMap);
						}
						if(((Map)triggerJobMap.get(key)).get(StrVal.MAP_KEY_FLAG).equals(StrVal.VAL_CONSTANT_Y)){
							if(hisTriggerM.containsKey(key)&&hisTriggerM.get(key).equals(initNextTriggertime)){
								log.info(key+" has finished today.expression["+val+"].");
							}else{
								String currenttime = timestamp.format(d);
								log.info(key+" is satisfy .expression["+val+"].");
								invokeJob(key);
								JobInfo.writeCTLFile(key+conf.get(StrVal.CTL_CONF_JOB_DELIMITER)+initNextTriggertime+conf.get(StrVal.SYSTEM_ROW_DELIMITER),conf.get(StrVal.TRIGGER_JOB_LOG_PATH)+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.TRIGGER_JOB_LOG_NAME)+"_"+yyyymmdd+".log");
							}
							Map tmpMap = (Map) triggerJobMap.get(key);
							tmpMap.put(StrVal.MAP_KEY_FLAG, StrVal.VAL_CONSTANT_N);
						}else{
							if(conf.get(StrVal.SYS_DEBUG_FLAG).equals(conf.get(StrVal.SYSTEM_SUCCESS_FLAG))){
								log.info(key+" has finished last time.expression["+val+"].");
							}
							continue;
						}
					}else{
						if(conf.get(StrVal.SYS_DEBUG_FLAG).equals(conf.get(StrVal.SYSTEM_SUCCESS_FLAG))){
							log.info(key+" is not satisfy .expression["+val+"].");
						}
						if(triggerJobMap.containsKey(key)){
							log.info(key+" has triggered.");
							JobInfo.writeCTLFile(key+conf.get(StrVal.CTL_CONF_JOB_DELIMITER)+nextTriggertime+conf.get(StrVal.SYSTEM_ROW_DELIMITER),conf.get(StrVal.TRIGGER_JOB_LOG_PATH)+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.TRIGGER_JOB_LOG_NAME)+"_"+yyyymmdd+".log");
							triggerJobMap.remove(key);
						}
					}
				}else if(triggertype.trim().toUpperCase().equals(StrVal.VAL_CONSTANT_SIMPLE)){
					
				}else{
					log.info(key+" is not cron or simple trigger.");
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
	public Map confFromFile(String filenm){
		Map map = new HashMap();
		String result = null;
		FileReader fileReader = null;
		BufferedReader bufferedReader = null;
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
				String[] keyval = read.split(conf.get(StrVal.CTL_CONF_JOB_DELIMITER));
				String ctl = keyval[0].trim().toUpperCase();
				String type = keyval[1];
				String expr = keyval[2];
				Map tmpM = new HashMap();
				tmpM.put(StrVal.MAP_KEY_TYPE, type);
				tmpM.put(StrVal.MAP_KEY_EXPRESSION, expr);
				map.put(ctl, tmpM);
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
		return map;
	}
	public Map logFromFile(String filenm){
		Map map = new HashMap();
		String result = null;
		FileReader fileReader = null;
		BufferedReader bufferedReader = null;
		File file = new File(filenm);
		if(file.exists()){
	      try {	
			String read = null;
			fileReader = new FileReader(file);
			bufferedReader = new BufferedReader(fileReader);
			while((read=bufferedReader.readLine())!=null){
				String[] keyval = read.split(conf.get(StrVal.CTL_CONF_JOB_DELIMITER));
				String ctl = keyval[0].trim().toUpperCase();
				String val = keyval[1];
				map.put(ctl, val);
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
		}
		return map;
	}
    public void triggerJobNoInfo(String ctl){
    	String mstPath = conf.get(StrVal.SYS_BATCH_PATH)+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_INSTANCE)+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_MASTER);
		String mstStreamPath = mstPath+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_STREAM);
		if(!new File(mstStreamPath).exists()){
			log.info("Path not exists "+mstStreamPath+".");
		}else{
			File stmfile = new File(mstStreamPath+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl);
			try {
				log.info("Trigger ctl "+ctl+",general file "+mstStreamPath+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl+".");
				stmfile.createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
    }
    public void triggerJob(String ctl){
    	String mstPath = conf.get(StrVal.SYS_BATCH_PATH)+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_INSTANCE)+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_MASTER);
		String mstStreamPath = mstPath+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_STREAM);
		if(conf.get(StrVal.SYS_DEBUG_FLAG).equals(conf.get(StrVal.SYSTEM_SUCCESS_FLAG))){
			log.info("trigger ctl "+ctl+".");
		}
		if(!new File(mstStreamPath).exists()){
			log.info("Path not exists "+mstStreamPath+".");
		}else{
			File stmfile = new File(mstStreamPath+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl);
			try {
				log.info("Trigger ctl "+ctl+",general file "+mstStreamPath+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl+".");
				stmfile.createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
    }
    public void invokeJob(String ctl){
    	String[] arr = ctl.split(conf.get(StrVal.SYSTEM_CONTROL_DELIMITER));
    	String batchnum = arr[0];
    	String control = arr[1];
    	if(conf.get(StrVal.SYS_DEBUG_FLAG).equals(conf.get(StrVal.SYSTEM_SUCCESS_FLAG))){
			log.info("invoke control "+control+".");
		}
    	if(jobconfMaping.containsKey(control)){
//    		String pool = (String) jobconfMaping.get(conf.get(StrVal.JOB_BASIC_POOL));
//			String system = (String) jobconfMaping.get(conf.get(StrVal.JOB_BASIC_SYSTEM));
//			String job = (String) jobconfMaping.get(conf.get(StrVal.JOB_BASIC_JOB));
    		Map tmpJobInfoMap = (Map) jobconfMaping.get(control);
			String nodeName = (String) tmpJobInfoMap.get(conf.get(StrVal.JOB_ATTR_NODE));
//			String jobType = (String) tmpJobInfoMap.get(conf.get(StrVal.JOB_ATTR_JOBTYPE));
//			String checkTimewindow = (String) tmpJobInfoMap.get(conf.get(StrVal.JOB_ATTR_CHECKTIMEWINDOW));
			String checkTimeTrigger = (String) tmpJobInfoMap.get(conf.get(StrVal.JOB_ATTR_CHECKTIMETRIGGER));
//			String checkCalendar = (String) tmpJobInfoMap.get(conf.get(StrVal.JOB_ATTR_CHECKCALENDAR));
//			String checkLastStatus = (String) tmpJobInfoMap.get(conf.get(StrVal.JOB_ATTR_CHECKLASTSTATUS));
//			String priority = (String) tmpJobInfoMap.get(conf.get(StrVal.JOB_ATTR_PRIORITY));
			String enable = (String) tmpJobInfoMap.get(conf.get(StrVal.JOB_ATTR_ENABLE));
			//
			if(!(enable.equals("1"))){
				log.info(ctl+" enable ["+enable+"] is not satisfy.");
				return;
			}
			if(!(checkTimeTrigger.equals(StrVal.VAL_CONSTANT_Y))){
				log.info(ctl+" timeTrigger ["+checkTimeTrigger+"] is not satisfy.");
				return;
			}
			SimpleDateFormat txdate_format = new SimpleDateFormat("yyyyMMdd");
			Date nowTime = new Date();
			String txdate = txdate_format.format(nowTime);
			//record
			String mstPath = conf.get(StrVal.SYS_BATCH_PATH)+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_INSTANCE)+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_MASTER);
			String mstStreamPath = mstPath+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_STREAM);
//			if(nodeName.length()>0){
//				log.info("Job info trigger ctl "+ctl+" to node "+nodeName+",general file "+mstStreamPath+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl+".");
//				JobInfo.writeCTLFile(conf.get(StrVal.SYSTEM_PARAMETER_NODE)+conf.get(StrVal.SYSTEM_KEYVAL_DELIMITER)+nodeName+conf.get(StrVal.SYSTEM_ROW_DELIMITER), mstStreamPath+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl);
//			}else{
//				log.info("Job no info trigger ctl "+ctl+",general file "+mstStreamPath+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl+".");
//				triggerJobNoInfo(ctl);
//			}
			triggerJobNoInfo(ctl);
    	}else{
    		log.info(ctl+" Info not exists config.");
//    		triggerJobNoInfo(ctl);
    	}
    }
}
