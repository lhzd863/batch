package com.batch.master;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.batch.util.Configuration;
import com.batch.util.JobInfo;
import com.batch.util.LoggerUtil;
import com.batch.util.StrVal;

public class StreamJob implements Runnable{

	Configuration conf = null;
	Map shareM = null;
	Map jobstatusM = null;
    Map jobsteamM = null;
    Map jobconfMaping = null;
    Map jobStepMaping = null;
    Map jobTimewindowMaping = null;
    
	Logger log = null;
	String cfg = "";
	final static String clss = "stream";
    public void initialize(String node,String cfgfile,Map shareM){
    	//init conf
    	this.cfg = cfgfile;
		this.conf = new Configuration();
		conf.initialize(new File(cfg));
		DefaultConf.initialize(conf);
		
    	this.shareM = shareM;
    	this.jobstatusM = (Map) this.shareM.get(StrVal.MAP_KEY_STATUS);
    	this.jobconfMaping = (Map) this.shareM.get(StrVal.MAP_KEY_JOB);
    	this.jobStepMaping = (Map) this.shareM.get(StrVal.MAP_KEY_STEP);
    	this.jobTimewindowMaping = (Map) this.shareM.get(StrVal.MAP_KEY_TIMEWINDOW);
    	
    	SimpleDateFormat time = new SimpleDateFormat("yyyyMMdd");
		Date nowTime = new Date();
		String yyyymmdd = time.format(nowTime);
		if(log==null){
			log = new LoggerUtil().getLoggerByName(conf.get(StrVal.SYS_LOG_PATH), clss+"_"+yyyymmdd);
		}
		
	}
	public void stream(String control){
		String ctl = control;
		String mstPath = conf.get(StrVal.SYS_BATCH_PATH)+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_INSTANCE)+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_MASTER);
		String mstStreamPath = mstPath+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_STREAM);
		String mstListPath = mstPath+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_LIST);
		//header
		Map ctlInfoMap = JobInfo.getCTLInfoMap(conf,ctl);
		String[] arrjob = ctl.split(conf.get(StrVal.SYSTEM_CONTROL_DELIMITER));
		String batchNum = arrjob[0];
		String t_pool = (String)ctlInfoMap.get(StrVal.MAP_KEY_POOL);
		String t_sys = (String)ctlInfoMap.get(StrVal.MAP_KEY_SYS);
		String t_job = (String)ctlInfoMap.get(StrVal.MAP_KEY_JOB);
		String t_ctl = t_pool+"_"+t_sys+"_"+t_job;
		//config
//		if(!jobconfMaping.containsKey(t_ctl)){
//			log.info(control+" not exist config, wait for next time!");
//			return;
//		}
//		//step
//		if(!jobStepMaping.containsKey(t_ctl)){
//			log.info(control+" not exist step, wait for next time!");
//			return;
//		}
//		//timewindow
//		if(!jobStepMaping.containsKey(t_ctl)){
//			log.info(control+" not exist timewindow, wait for next time!.");
//			return;
//		}
		
		//read map
		SimpleDateFormat time = new SimpleDateFormat("yyyy-MM-dd HH:mm:SS");
		Date nowTime = new Date();
		String now = time.format(nowTime);
		Map streamCtlMap = JobInfo.readCTLFile(conf,mstStreamPath+conf.get(StrVal.SYS_DIR_DELIMITER)+control);
		
		log.info("Update "+ctl+" status to "+conf.get(StrVal.SYSTEM_STATUS_READY)+" .");
		streamCtlMap.put(conf.get(StrVal.SYSTEM_PARAMETER_STATUS), conf.get(StrVal.SYSTEM_STATUS_READY));
		JobInfo.writeCTLFile(conf.get(StrVal.SYSTEM_PARAMETER_STATUS)+conf.get(StrVal.SYSTEM_KEYVAL_DELIMITER)+conf.get(StrVal.SYSTEM_STATUS_READY)+conf.get(StrVal.SYSTEM_ROW_DELIMITER), mstStreamPath+conf.get(StrVal.SYS_DIR_DELIMITER)+control);
		//node
		if(jobconfMaping.containsKey(t_ctl)){
			Map t_jobconfigMap = (Map) jobconfMaping.get(t_ctl);
			String tmpNode = (String) t_jobconfigMap.get(conf.get(StrVal.JOB_ATTR_NODE));
			if(tmpNode!=null&&tmpNode.length()>0){
				JobInfo.writeCTLFile(conf.get(StrVal.SYSTEM_PARAMETER_NODE)+conf.get(StrVal.SYSTEM_KEYVAL_DELIMITER)+tmpNode+conf.get(StrVal.SYSTEM_ROW_DELIMITER), mstStreamPath+conf.get(StrVal.SYS_DIR_DELIMITER)+control);
			}
		}
		//cmd
		if(jobStepMaping.containsKey(t_ctl)){
			Map t_jobstepMap = (Map) jobStepMaping.get(t_ctl);
			String tmpcmd = (String) t_jobstepMap.get(conf.get(StrVal.JOB_ATTR_CMD));
			if(tmpcmd!=null&&tmpcmd.length()>0){
				JobInfo.writeCTLFile(conf.get(StrVal.SYSTEM_PARAMETER_CMD)+conf.get(StrVal.SYSTEM_KEYVAL_DELIMITER)+tmpcmd+conf.get(StrVal.SYSTEM_ROW_DELIMITER), mstStreamPath+conf.get(StrVal.SYS_DIR_DELIMITER)+control);
			}
		}
		log.info("Move "+control+" from "+mstStreamPath+" to "+mstListPath+".");
		new File(mstStreamPath+conf.get(StrVal.SYS_DIR_DELIMITER)+control).renameTo(new File(mstListPath+conf.get(StrVal.SYS_DIR_DELIMITER)+control));
		if(new File(mstStreamPath+conf.get(StrVal.SYS_DIR_DELIMITER)+control).exists()){
			log.info("Delete "+control+" from "+mstStreamPath+".");
			new File(mstStreamPath+conf.get(StrVal.SYS_DIR_DELIMITER)+control).delete();
		}
		jobstatusM.put(ctl,streamCtlMap);
	}

	@Override
	public void run() {
		long starttime = System.currentTimeMillis();
		while(true){
			//init log
			SimpleDateFormat time = new SimpleDateFormat("yyyyMMdd");
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
			}
			
			String mstPath = conf.get(StrVal.SYS_BATCH_PATH)+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_INSTANCE)+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_MASTER);
			String mstStreamPath = mstPath+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_STREAM);
			
			if(conf.get(StrVal.SYS_DEBUG_FLAG).equals(conf.get(StrVal.SYSTEM_SUCCESS_FLAG))){
				log.info("Check directory "+mstStreamPath+".");
			}
			if(!(conf.get(StrVal.SYSTEM_RUNNING_FLAG).equals("0"))){
				log.info("Stream job exit running flag is "+conf.get(StrVal.SYSTEM_RUNNING_FLAG)+".");
				break;
			}
			File streamPath = new File(mstStreamPath);
			String[] streamlist = streamPath.list();
			for(int i=0;i<streamlist.length;i++){
				String controlFile = streamlist[i];
				if(conf.get(StrVal.SYS_DEBUG_FLAG).equals(conf.get(StrVal.SYSTEM_SUCCESS_FLAG))){
					log.info("Stream job received "+streamlist[i]+".");
				}
				if(jobstatusM.containsKey(controlFile)){
					Map tmpMap = (Map) jobstatusM.get(controlFile);
					log.info(controlFile +" has stream status ["+tmpMap.get(conf.get(StrVal.SYSTEM_PARAMETER_STATUS))+"].waite for next time.");
				}else{
					stream(controlFile);
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
}
