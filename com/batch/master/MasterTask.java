package com.batch.master;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import com.batch.util.ClearLog;
import com.batch.util.Configuration;
import com.batch.util.LoggerUtil;
import com.batch.util.StrVal;

public class MasterTask {
    Map jobstatusM = null;
    Map jobsteamM = null;
    Map serverstatusM = null;
    Map jobserverMaping = null;
    Map shareM = null;
    Map ctlInfoM = null;
    
    //map
    Map jobconfMaping = null;
    Map jobTriggerMaping = null;
    Map jobStreamMapping = null;
    Map jobDependencyMaping = null;
    Map jobStepMaping = null;
    Map jobTimewindowMaping = null;
    
	public void initialize(String node,String cfg){
		Configuration conf = new Configuration();
		conf.initialize(new File(cfg));
		DefaultConf.initialize(conf);
		//initialize Map
		shareM = new ConcurrentHashMap();
		jobstatusM = new ConcurrentHashMap();
		jobsteamM = new ConcurrentHashMap();
		serverstatusM = new ConcurrentHashMap();
		ctlInfoM = new ConcurrentHashMap();
		jobserverMaping = new ConcurrentHashMap();
		//conf
		jobconfMaping = new ConcurrentHashMap();
		jobTriggerMaping = new ConcurrentHashMap();
		jobStreamMapping = new ConcurrentHashMap();
		jobDependencyMaping = new ConcurrentHashMap();
		jobStepMaping = new ConcurrentHashMap();
		jobTimewindowMaping = new ConcurrentHashMap();
		
		shareM.put(StrVal.MAP_KEY_STATUS, jobstatusM);
		shareM.put(StrVal.MAP_KEY_STREAM, jobsteamM);
		shareM.put(StrVal.MAP_KEY_NODESTATUS, serverstatusM);
		shareM.put(StrVal.MAP_KEY_MAPPING, jobserverMaping);
		shareM.put(StrVal.MAP_KEY_CTL, ctlInfoM);
		//JOB MAP SHARE
		shareM.put(StrVal.MAP_KEY_JOB, jobconfMaping);
		shareM.put(StrVal.MAP_KEY_STREAM, jobStreamMapping);
		shareM.put(StrVal.MAP_KEY_DEPENDENCY, jobDependencyMaping);
		shareM.put(StrVal.MAP_KEY_STEP, jobStepMaping);
		shareM.put(StrVal.MAP_KEY_TIMEWINDOW, jobTimewindowMaping);
		shareM.put(StrVal.MAP_KEY_TRIGGER, jobTriggerMaping);
		
		//作业属性
		LoadInfo loadjob = new LoadInfo();
		loadjob.initialize(node, cfg, shareM);
		new Thread(loadjob).start();
		//作业状态
		SaveJobStatus sjs = new SaveJobStatus();
		sjs.initialize(node,cfg, shareM);
		new Thread(sjs).start();
		//统计资源信息
		ResourceCollect rc = new ResourceCollect();
		rc.initialize(node,cfg, shareM);
		new Thread(rc).start();
		//时间触发作业
		TimeTiggerJob ttj = new TimeTiggerJob();
		ttj.initialize(node,cfg, shareM);
		new Thread(ttj).start();
		//目录触发作业
		StreamJob sj = new StreamJob();
		sj.initialize(node,cfg, shareM);
		new Thread(sj).start();
		//notic
		TaskStatus ns = new TaskStatus();
		ns.initialize(node,cfg, shareM);
		new Thread(ns).start();
		//log
		ClearLog cl= new ClearLog();
		cl.initialize(node,cfg, shareM);
		new Thread(cl).start();
		
	}
	private static void printUsage(String node,String cfg) {
	    System.out.println("*******************************************************************");
	    System.out.println("*Usage: java -jar *.jar cfg node");
	    System.out.println("* node:"+node);
	    System.out.println("* cfg :"+cfg);
	    System.out.println("*******************************************************************");
	}
	public static void main(String[] args) {
		String cfg = args[0];//"D:/conf/core-batch.cfg";
		String node = args[1];//"master-1";
		printUsage(node,cfg);
		MasterTask mt = new MasterTask();
		mt.initialize(node, cfg);
	}
}
