package com.batch.master;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import com.batch.util.Configuration;
import com.batch.util.JobInfo;
import com.batch.util.LoggerUtil;
import com.batch.util.StrVal;

public class TaskStatus implements Runnable{

	Configuration conf = null;
	Logger log = null;
	Map shareM = null;
	Map jobstatusM = null;
    Map jobsteamM = null;
    Map pendingM = null;
    Map serverstatusM = null;
    Map jobserverMaping = null;
    String cfg = "";
    
    int cpulmt = 0;
	int memlmt = 0;
	int prclmt = 0;
	int maxres = 0;
	final static String clss = "task";
	public void initialize(String node,String cfgfile,Map shareM){
		//init conf
		this.cfg = cfgfile;
		this.conf = new Configuration();
		conf.initialize(new File(cfg));
		DefaultConf.initialize(conf);
		
    	this.shareM = shareM;
    	this.jobstatusM = (Map) this.shareM.get(StrVal.MAP_KEY_STATUS);
    	this.serverstatusM = (Map) this.shareM.get(StrVal.MAP_KEY_NODESTATUS);
    	this.jobserverMaping = (Map) this.shareM.get(StrVal.MAP_KEY_MAPPING);
    	
    	//
    	cpulmt = Integer.parseInt(conf.get(StrVal.SYS_LIMIT_SLAVE_CPU));
		memlmt = Integer.parseInt(conf.get(StrVal.SYS_LIMIT_SLAVE_MEM));
		prclmt = Integer.parseInt(conf.get(StrVal.SYS_LIMIT_SLAVE_PRC));
		maxres = 100+100+prclmt;
		
    	SimpleDateFormat time = new SimpleDateFormat("yyyyMMdd");
		Date nowTime = new Date();
		String yyyymmdd = time.format(nowTime);
		if(log==null){
			log = new LoggerUtil().getLoggerByName(conf.get("sys.log.path"), clss+"_"+yyyymmdd);
		}
		
		
	}
	public boolean isDenpendencyOK(String ctl){
		return true;
	}
	
	public void list2notic(String ctl){
		String mstListPath = conf.get(StrVal.SYS_BATCH_PATH)+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_INSTANCE)+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_MASTER)+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_LIST);
		String mstNoticePath = conf.get(StrVal.SYS_BATCH_PATH)+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_INSTANCE)+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_MASTER)+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_NOTICE);
		
		//multi
		if(isDenpendencyOK(ctl)){
			//notice
			synchronized(jobstatusM){
				Map statusMap = (Map) jobstatusM.get(ctl);
				// list -> notice
				log.info("Move file "+ctl+" from "+mstListPath+" to "+mstNoticePath+" .");
				if(new File(mstNoticePath+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl).exists()){
					log.info(mstNoticePath+" exists ctl delete "+ctl+".");
					new File(mstNoticePath+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl).delete();
				}
				JobInfo.writeCTLFile(conf.get(StrVal.SYSTEM_PARAMETER_STATUS)+conf.get(StrVal.SYSTEM_KEYVAL_DELIMITER)+conf.get(StrVal.SYSTEM_STATUS_PENDING)+conf.get(StrVal.SYSTEM_ROW_DELIMITER), mstListPath+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl);
				new File(mstListPath+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl).renameTo(new File(mstNoticePath+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl));
				// Ready -> Pending
				log.info("Update Status "+ctl+" from "+conf.get(StrVal.SYSTEM_STATUS_READY)+" to "+conf.get(StrVal.SYSTEM_STATUS_PENDING)+" .");
				//update status
				statusMap.put(conf.get(StrVal.SYSTEM_PARAMETER_STATUS), conf.get(StrVal.SYSTEM_STATUS_PENDING));
				jobstatusM.put(ctl,statusMap);
			}
		}
	}
	public void choseServer(String ctl){
		String servername=null;
		int culval = maxres;
		String slvPath = conf.get(StrVal.SYS_BATCH_PATH)+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_INSTANCE)+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_SLAVE);
		String mstPath = conf.get(StrVal.SYS_BATCH_PATH)+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_INSTANCE)+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_MASTER);
		String mstlockPath = mstPath+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_MASTER_LOCK);
		String mstnoticePath = mstPath+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_NOTICE);
		Map jobserverinfo = null;
		
		Map mstnoticemap = JobInfo.readCTLFile(conf,mstnoticePath + conf.get(StrVal.SYS_DIR_DELIMITER) + ctl);
		if(mstnoticemap.containsKey(conf.get(StrVal.SYSTEM_PARAMETER_NODE))&&
		   ((String)mstnoticemap.get(conf.get(StrVal.SYSTEM_PARAMETER_NODE))).length()>0){
			String jobnode = (String) mstnoticemap.get(conf.get(StrVal.SYSTEM_PARAMETER_NODE));
			String tmpslvctl = slvPath+conf.get(StrVal.SYS_DIR_DELIMITER)+jobnode;
			File tmpslvctlf = new File(tmpslvctl);
			if(!(tmpslvctlf.isDirectory())){
				log.info("Directory not exists "+slvPath+conf.get(StrVal.SYS_DIR_DELIMITER)+jobnode+".");
				return;
			}
			String slvctl = tmpslvctl+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl;
			File slvctlf = new File(slvctl);
			if(slvctlf.exists()){
				mvNotice2Lock(ctl,jobnode);
			}else{
				log.info(jobnode+" has not submited ctl "+ctl+". wait for next time!");
			}
			return;
		}
		
		Iterator iter = serverstatusM.entrySet().iterator();
		while(iter.hasNext()){
			Entry entry = (Entry) iter.next();
			String key = (String) entry.getKey();
			Map infoM = (Map) entry.getValue();
			int tmpculval = resourceVal(key,infoM,1,1);
			
			String tmpslvctl = slvPath+conf.get(StrVal.SYS_DIR_DELIMITER)+key;
			File tmpslvctlf = new File(tmpslvctl);
			if(!(tmpslvctlf.exists())){
				log.info("Not exists =>"+slvPath+conf.get(StrVal.SYS_DIR_DELIMITER)+key);
				continue;
			}
			if(!(tmpslvctlf.isDirectory())){
				log.info("Directory not exists "+slvPath+conf.get(StrVal.SYS_DIR_DELIMITER)+key);
				continue;
			}
			String slvctl = tmpslvctl+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl;
			File slvctlf = new File(slvctl);
			if(slvctlf.exists()){
				log.info(key+" has submited ctl "+ctl);
			}
			if(tmpculval<culval){
				culval = tmpculval;
				servername = key;
			}
		}
		if(servername!=null){
			log.info("Master submit ctl "+ctl+" to "+servername+".");
			if(new File(mstlockPath+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl).exists()){
				log.info(mstlockPath+" exists ctl delete "+ctl+".");
				new File(mstlockPath+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl).delete();
			}
			mvNotice2Lock(ctl,servername);
		}else{
			if(conf.get(StrVal.SYS_DEBUG_FLAG).equals(conf.get(StrVal.SYSTEM_SUCCESS_FLAG))){
                log.info(ctl+" server name not exists.");
			}
		}
	}

	public void checkServerJobMaping(String ctl){
		String mstPath = conf.get(StrVal.SYS_BATCH_PATH)+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_INSTANCE)+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_MASTER);
		String mstLockPath = mstPath+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_MASTER_LOCK);
		String mstnoticePath = mstPath+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_NOTICE);
		String slvPath = conf.get(StrVal.SYS_BATCH_PATH)+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_INSTANCE)+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_SLAVE);
		
		if(jobserverMaping.containsKey(ctl)){
			Map tmpjobserverinfo = (Map) jobserverMaping.get(ctl);
			long tmpstarttime = (Long)tmpjobserverinfo.get(conf.get(StrVal.SYSTEM_PARAMETER_STARTTIME));
			long tmpendtime = System.currentTimeMillis();
			long tmpdivi = tmpendtime - tmpstarttime;
			if(tmpdivi>Long.parseLong(conf.get(StrVal.SYS_LIMIT_JOB_TIME))){
				log.info(ctl+" job time "+tmpdivi+",gt "+conf.get(StrVal.SYS_LIMIT_JOB_TIME)+".");
				Map mstmap = JobInfo.readCTLFile(conf,mstLockPath + conf.get(StrVal.SYS_DIR_DELIMITER) + ctl);
				mstmap.put(conf.get(StrVal.SYSTEM_PARAMETER_STATUS), conf.get(StrVal.SYSTEM_STATUS_FAIL));
				jobstatusM.put(ctl,mstmap);
				JobInfo.writeCTLFile(conf.get(StrVal.SYSTEM_PARAMETER_STATUS)+conf.get(StrVal.SYSTEM_KEYVAL_DELIMITER)+conf.get(StrVal.SYSTEM_STATUS_FAIL)+conf.get(StrVal.SYSTEM_ROW_DELIMITER), mstLockPath+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl);
				jobserverMaping.remove(ctl);
				log.info("Update "+ctl+" status ["+conf.get(StrVal.SYSTEM_STATUS_FAIL)+"],submit time gt "+conf.get(StrVal.SYS_LIMIT_JOB_TIME)+".");
				return;
			}
			String tmpnodename = (String) tmpjobserverinfo.get(conf.get(StrVal.SYSTEM_PARAMETER_NODE));
			String slvnodepath = slvPath + conf.get(StrVal.SYS_DIR_DELIMITER) + tmpnodename;
			if(new File(slvnodepath).exists()){
				String slvStatusCtlpath = slvnodepath+ conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_STATUS)+ conf.get(StrVal.SYS_DIR_DELIMITER)+ ctl;
				if(new File(slvStatusCtlpath).exists()){
					Map slvmap = JobInfo.readCTLFile(conf,slvStatusCtlpath);
					Map mstmap = JobInfo.readCTLFile(conf,mstLockPath + conf.get(StrVal.SYS_DIR_DELIMITER) + ctl);
					if(slvmap.containsKey(conf.get(StrVal.SYSTEM_PARAMETER_STATUS))&&
					   slvmap.containsKey(conf.get(StrVal.SYSTEM_PARAMETER_SEQUENCE))&&
					   mstmap.containsKey(conf.get(StrVal.SYSTEM_PARAMETER_SEQUENCE))&&
					   slvmap.get(conf.get(StrVal.SYSTEM_PARAMETER_SEQUENCE)).equals(mstmap.get(conf.get(StrVal.SYSTEM_PARAMETER_SEQUENCE)))  ){
						  mstmap.put(conf.get(StrVal.SYSTEM_PARAMETER_STATUS),slvmap.get(conf.get(StrVal.SYSTEM_PARAMETER_STATUS)));
						  jobstatusM.put(ctl,mstmap);
						  if(conf.get(StrVal.SYS_DEBUG_FLAG).equals(conf.get(StrVal.SYSTEM_SUCCESS_FLAG))){
							  log.info("slv seq ["+slvmap.get(conf.get(StrVal.SYSTEM_PARAMETER_SEQUENCE))+"],mst seq ["+mstmap.get(conf.get(StrVal.SYSTEM_PARAMETER_SEQUENCE))+"].");
						  }
					      log.info("Update status "+ctl+" from "+conf.get(StrVal.SYSTEM_STATUS_SUBMIT)+" to "+slvmap.get(conf.get(StrVal.SYSTEM_PARAMETER_STATUS))+".");
					}else{
						if(new File(slvnodepath+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl).exists()){
							if(conf.get(StrVal.SYS_DEBUG_FLAG).equals(conf.get(StrVal.SYSTEM_SUCCESS_FLAG))){
								log.info(tmpnodename+" Slave not running job "+ctl+".");
							}
						}else{
							log.info(ctl+" all not exists "+slvStatusCtlpath+" and "+slvnodepath+",delete from job node mapping.");
							jobserverMaping.remove(ctl);
							new File(mstLockPath+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl).renameTo(new File(mstnoticePath+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl));
							JobInfo.writeCTLFile(conf.get(StrVal.SYSTEM_PARAMETER_STATUS)+conf.get(StrVal.SYSTEM_KEYVAL_DELIMITER)+conf.get(StrVal.SYSTEM_STATUS_PENDING)+conf.get(StrVal.SYSTEM_ROW_DELIMITER), mstnoticePath+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl);
							log.info("Move "+ctl+" from "+mstLockPath+" to "+mstnoticePath+".");
							mstmap.put(conf.get(StrVal.SYSTEM_PARAMETER_STATUS), conf.get(StrVal.SYSTEM_STATUS_PENDING));
							log.info("Update "+ctl+" from "+conf.get(StrVal.SYSTEM_STATUS_SUBMIT)+" to "+conf.get(StrVal.SYSTEM_STATUS_PENDING)+".");
							jobstatusM.put(ctl,mstmap);
							return;
					   }
					}
				}else{
					String slvCtlPath = slvnodepath+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl;					
					//slv status  and node not exists
					if(!new File(slvCtlPath+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl).exists()){
						log.info(ctl+" all not exists "+slvCtlPath+" and "+slvStatusCtlpath+",delete from job node mapping.");
						jobserverMaping.remove(ctl);
						Map map = JobInfo.readCTLFile(conf,mstLockPath+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl);
						new File(mstLockPath+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl).renameTo(new File(mstnoticePath+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl));
						JobInfo.writeCTLFile(conf.get(StrVal.SYSTEM_PARAMETER_STATUS)+conf.get(StrVal.SYSTEM_KEYVAL_DELIMITER)+conf.get(StrVal.SYSTEM_STATUS_PENDING)+conf.get(StrVal.SYSTEM_ROW_DELIMITER), mstnoticePath+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl);
						log.info("Move "+ctl+" from "+mstLockPath+" to "+mstnoticePath+".");
						map.put(conf.get(StrVal.SYSTEM_PARAMETER_STATUS), conf.get(StrVal.SYSTEM_STATUS_PENDING));
						log.info("Update "+ctl+" from "+conf.get(StrVal.SYSTEM_STATUS_SUBMIT)+" to "+conf.get(StrVal.SYSTEM_STATUS_PENDING)+".");
						jobstatusM.put(ctl,map);
						return;
					}
					log.info(ctl+" has submit to "+tmpnodename+".");
				}
			}else{
				String slvStatusCtlpath = slvnodepath+ conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_STATUS)+ conf.get(StrVal.SYS_DIR_DELIMITER)+ ctl;
				if(new File(slvStatusCtlpath).exists()){
					Map slvmap = JobInfo.readCTLFile(conf,slvStatusCtlpath);
					Map mstmap = JobInfo.readCTLFile(conf,mstLockPath + conf.get(StrVal.SYS_DIR_DELIMITER) + ctl);
					if(slvmap.containsKey(conf.get(StrVal.SYSTEM_PARAMETER_STATUS))&&
					   slvmap.containsKey(conf.get(StrVal.SYSTEM_PARAMETER_SEQUENCE))&&
					   mstmap.containsKey(conf.get(StrVal.SYSTEM_PARAMETER_SEQUENCE))&&
					   slvmap.get(conf.get(StrVal.SYSTEM_PARAMETER_SEQUENCE)).equals(mstmap.get(conf.get(StrVal.SYSTEM_PARAMETER_SEQUENCE)))  ){
						  mstmap.put(conf.get(StrVal.SYSTEM_PARAMETER_STATUS),slvmap.get(conf.get(StrVal.SYSTEM_PARAMETER_STATUS)));
						  jobstatusM.put(ctl,mstmap);
						  if(conf.get(StrVal.SYS_DEBUG_FLAG).equals(conf.get(StrVal.SYSTEM_SUCCESS_FLAG))){
								log.info("slv seq ["+slvmap.get(conf.get(StrVal.SYSTEM_PARAMETER_SEQUENCE))+"],mst seq ["+mstmap.get(conf.get(StrVal.SYSTEM_PARAMETER_SEQUENCE))+"].");
						  }
					      log.info("Update status "+ctl+" from "+conf.get(StrVal.SYSTEM_STATUS_SUBMIT)+" to "+slvmap.get(conf.get(StrVal.SYSTEM_PARAMETER_STATUS))+".");
					}else{
						if(conf.get(StrVal.SYS_DEBUG_FLAG).equals(conf.get(StrVal.SYSTEM_SUCCESS_FLAG))){
							log.info(ctl+" not submit.");
						}
					}
				}else{
					if(conf.get(StrVal.SYS_DEBUG_FLAG).equals(conf.get(StrVal.SYSTEM_SUCCESS_FLAG))){
						log.info(ctl+" not submit.");
					}
				}
			}
		}else{
			Map map = JobInfo.readCTLFile(conf,mstLockPath+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl);
			if(new File(mstnoticePath+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl).exists()){
				log.info(mstnoticePath+" exists ctl delete "+ctl+".");
				new File(mstnoticePath+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl).delete();
			}
			new File(mstLockPath+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl).renameTo(new File(mstnoticePath+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl));
			log.info("Move "+ctl+" from "+mstLockPath+" to "+mstnoticePath+".");
			map.put(conf.get(StrVal.SYSTEM_PARAMETER_STATUS), conf.get(StrVal.SYSTEM_STATUS_PENDING));
			jobstatusM.put(ctl,map);
		}
	}
	
	public void checkRunningJob(String ctl){
		String mstPath = conf.get(StrVal.SYS_BATCH_PATH)+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_INSTANCE)+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_MASTER);
		String mstLockPath = mstPath+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_MASTER_LOCK);
		String mstnoticePath = mstPath+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_NOTICE);
		String slvPath = conf.get(StrVal.SYS_BATCH_PATH)+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_INSTANCE)+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_SLAVE);
		if(jobserverMaping.containsKey(ctl)){
			Map tmpjobserverinfo = (Map) jobserverMaping.get(ctl);
			long tmpstarttime = (Long)tmpjobserverinfo.get(conf.get(StrVal.SYSTEM_PARAMETER_STARTTIME));
			long tmpendtime = System.currentTimeMillis();
			long tmpdivi = tmpendtime - tmpstarttime;

			String nodename= (String) tmpjobserverinfo.get(conf.get(StrVal.SYSTEM_PARAMETER_NODE));
			String slvnodepath = slvPath + conf.get(StrVal.SYS_DIR_DELIMITER) + nodename;
			String slvnodestatuspath = slvnodepath + conf.get(StrVal.SYS_DIR_DELIMITER) + conf.get(StrVal.SYSTEM_VAR_STATUS);
			String slvnodectl = slvPath + conf.get(StrVal.SYS_DIR_DELIMITER) + ctl;
			String slvnodestatusctl = slvnodestatuspath + conf.get(StrVal.SYS_DIR_DELIMITER) + ctl;
			if(new File(slvnodestatusctl).exists()){
				Map slvmap = JobInfo.readCTLFile(conf,slvnodestatusctl);
				Map mstmap = JobInfo.readCTLFile(conf,mstLockPath + conf.get(StrVal.SYS_DIR_DELIMITER) + ctl);
				if(slvmap==null||mstmap==null){
					Map tmpMap = (Map) jobstatusM.get(ctl);
					tmpMap.put(conf.get(StrVal.SYSTEM_PARAMETER_STATUS), conf.get(StrVal.SYSTEM_STATUS_FAIL));
					jobstatusM.put(ctl,tmpMap);
				    log.info("Update status "+ctl+" status ["+conf.get(StrVal.SYSTEM_STATUS_FAIL)+"],"+slvnodepath+" or "+mstLockPath+" not attribute record.");
				}else{
					if(slvmap.containsKey(conf.get(StrVal.SYSTEM_PARAMETER_STATUS))&&
					   slvmap.containsKey(conf.get(StrVal.SYSTEM_PARAMETER_SEQUENCE))&&
					   mstmap.containsKey(conf.get(StrVal.SYSTEM_PARAMETER_SEQUENCE))&&
					   slvmap.get(conf.get(StrVal.SYSTEM_PARAMETER_SEQUENCE)).equals(mstmap.get(conf.get(StrVal.SYSTEM_PARAMETER_SEQUENCE)))  ){
						  if(mstmap.get(conf.get(StrVal.SYSTEM_PARAMETER_STATUS)).equals(conf.get(StrVal.SYSTEM_STATUS_SUBMIT))){
							  log.info("Update status "+ctl+" from "+conf.get(StrVal.SYSTEM_STATUS_SUBMIT)+" to "+slvmap.get(conf.get(StrVal.SYSTEM_PARAMETER_STATUS))+".");
						  }
						  mstmap.put(conf.get(StrVal.SYSTEM_PARAMETER_STATUS),slvmap.get(conf.get(StrVal.SYSTEM_PARAMETER_STATUS)));
						  jobstatusM.put(ctl,mstmap);
						  if(conf.get(StrVal.SYS_DEBUG_FLAG).equals(conf.get(StrVal.SYSTEM_SUCCESS_FLAG))){
							 log.info("slv seq ["+slvmap.get(conf.get(StrVal.SYSTEM_PARAMETER_SEQUENCE))+"],mst seq ["+mstmap.get(conf.get(StrVal.SYSTEM_PARAMETER_SEQUENCE))+"].");
						  }
					}else{
						Map tmpMap = (Map) jobstatusM.get(ctl);
						tmpMap.put(conf.get(StrVal.SYSTEM_PARAMETER_STATUS), conf.get(StrVal.SYSTEM_STATUS_FAIL));
						jobstatusM.put(ctl,tmpMap);
					    log.info("Update "+ctl+" status ["+conf.get(StrVal.SYSTEM_STATUS_FAIL)+"],"+slvnodestatuspath+" not exists.");
					}
				}
			}else{
				Map tmpMap = (Map) jobstatusM.get(ctl);
				tmpMap.put(conf.get(StrVal.SYSTEM_PARAMETER_STATUS), conf.get(StrVal.SYSTEM_STATUS_FAIL));
				jobstatusM.put(ctl,tmpMap);
			    log.info("Update "+ctl+" status ["+conf.get(StrVal.SYSTEM_STATUS_FAIL)+"],"+slvnodestatuspath+" not exists.");
			}
		}else{
			Map tmpMap = (Map) jobstatusM.get(ctl);
			tmpMap.put(conf.get(StrVal.SYSTEM_PARAMETER_STATUS), conf.get(StrVal.SYSTEM_STATUS_FAIL));
			jobstatusM.put(ctl,tmpMap);
		    log.info("Update status "+ctl+" status ["+conf.get(StrVal.SYSTEM_STATUS_FAIL)+"],job mapping server not exists.");
		}
	}
	
	@Override
	public void run() {
		long starttime = System.currentTimeMillis();
		//
		boolean flg = true;
		//path
		String mstPath = conf.get(StrVal.SYS_BATCH_PATH)+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_INSTANCE)+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_MASTER);
		String mstListPath = mstPath+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_LIST);
		String mstStatusPath = mstPath+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_STATUS);
		String mstNoticePath = mstPath+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_NOTICE);
		String mstLockPath = mstPath+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_MASTER_LOCK);
		
		while(true){
			//init log
			SimpleDateFormat time = new SimpleDateFormat("yyyyMMdd");
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
				flg = true;
			}
			//
	    	if(flg){
	    		flg = false;
	    		cpulmt = Integer.parseInt(conf.get(StrVal.SYS_LIMIT_SLAVE_CPU));
				memlmt = Integer.parseInt(conf.get(StrVal.SYS_LIMIT_SLAVE_MEM));
				prclmt = Integer.parseInt(conf.get(StrVal.SYS_LIMIT_SLAVE_PRC));
				maxres = 100+100+prclmt;
				
				mstPath = conf.get(StrVal.SYS_BATCH_PATH)+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_INSTANCE)+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_MASTER);
				mstListPath = mstPath+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_LIST);
				mstStatusPath = mstPath+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_STATUS);
				mstNoticePath = mstPath+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_NOTICE);
				mstLockPath = mstPath+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_MASTER_LOCK);
	    	}
			
			if(conf.get(StrVal.SYS_DEBUG_FLAG).equals(conf.get(StrVal.SYSTEM_SUCCESS_FLAG))){
				log.info("Check memory ctl.");
			}
			Iterator iter = jobstatusM.entrySet().iterator();
			while(iter.hasNext()){
				Entry entry = (Entry) iter.next();
				String key = (String) entry.getKey();
				Map valMap = (Map) entry.getValue();
				String jobstatus = (String) valMap.get(conf.get(StrVal.SYSTEM_PARAMETER_STATUS));
				if(conf.get(StrVal.SYS_DEBUG_FLAG).equals(conf.get(StrVal.SYSTEM_SUCCESS_FLAG))){
					log.info("Check control "+key+",status["+jobstatus+"].");
				}
				if(jobstatus.equals(conf.get(StrVal.SYSTEM_STATUS_READY))){
					if(!(new File(mstListPath+conf.get(StrVal.SYS_DIR_DELIMITER)+key)).exists()){
						log.info(key+" ignore "+conf.get(StrVal.SYSTEM_STATUS_READY)+" status.");
						jobstatusM.remove(key);
					}else{
						list2notic(key);
					}
				}else if(jobstatus.equals(conf.get(StrVal.SYSTEM_STATUS_PENDING))){
					if(!(new File(mstNoticePath+conf.get(StrVal.SYS_DIR_DELIMITER)+key)).exists()){
						log.info(key+" ignore "+conf.get(StrVal.SYSTEM_STATUS_PENDING)+" status.");
						jobstatusM.remove(key);
					}else{
						choseServer(key);
					}
				}else if(jobstatus.equals(conf.get(StrVal.SYSTEM_STATUS_SUBMIT))){
					if(!(new File(mstLockPath+conf.get(StrVal.SYS_DIR_DELIMITER)+key)).exists()){
						log.info(key+" ignore "+conf.get(StrVal.SYSTEM_STATUS_SUBMIT)+" status.");
						jobstatusM.remove(key);
					}else{
						checkServerJobMaping(key);
					}
					
				}else if(jobstatus.equals(conf.get(StrVal.SYSTEM_STATUS_RUNNING))){
					if(!(new File(mstLockPath+conf.get(StrVal.SYS_DIR_DELIMITER)+key)).exists()){
						log.info(key+" ignore "+conf.get(StrVal.SYSTEM_STATUS_RUNNING)+" status.");
						jobstatusM.remove(key);
					}else{
						checkRunningJob(key);
					}
				}else if(jobstatus.equals(conf.get(StrVal.SYSTEM_STATUS_SUCCESS))||jobstatus.equals(conf.get(StrVal.SYSTEM_STATUS_FAIL))){
					if(conf.get(StrVal.SYS_DEBUG_FLAG).equals(conf.get(StrVal.SYSTEM_SUCCESS_FLAG))){
						log.info(key+" status ["+jobstatus+"].");
					}
					continue;
				}else{
					log.info("Delete "+key+" from memory,status ["+jobstatus+"].");
					jobstatusM.remove(key);
				}
			}
			//list
			if(conf.get(StrVal.SYS_DEBUG_FLAG).equals(conf.get(StrVal.SYSTEM_SUCCESS_FLAG))){
				log.info("Check directory "+mstListPath+".");
			}
			String[] mstlist = new File(mstListPath).list();
			for(int i=0;i<mstlist.length;i++){
				String ctl = mstlist[i];
				if(new File(mstListPath+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl).isDirectory()){
					if(conf.get(StrVal.SYS_DEBUG_FLAG).equals(conf.get(StrVal.SYSTEM_SUCCESS_FLAG))){
						log.info(mstListPath+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl+" is directory ,directory will ignore.");
					}
					continue;
				}
				if(!jobstatusM.containsKey(ctl)){
					log.info("Mem not exist "+ctl+", put "+conf.get(StrVal.SYSTEM_STATUS_READY)+" status to Mem.");
					Map map = JobInfo.readCTLFile(conf,mstListPath+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl);
					map.put(conf.get(StrVal.SYSTEM_PARAMETER_STATUS), conf.get(StrVal.SYSTEM_STATUS_READY));
					jobstatusM.put(ctl,map);
				}else{ 
					if(conf.get(StrVal.SYS_DEBUG_FLAG).equals(conf.get(StrVal.SYSTEM_SUCCESS_FLAG))){
						log.info("Mem exists control "+ctl+".");
				    }
				}
			}
			//notice
			if(conf.get(StrVal.SYS_DEBUG_FLAG).equals(conf.get(StrVal.SYSTEM_SUCCESS_FLAG))){
				log.info("Check directory "+mstNoticePath+".");
			}
			String[] mstnotice = new File(mstNoticePath).list();
			for(int i=0;i<mstnotice.length;i++){
				String ctl = mstnotice[i];
				if(new File(mstNoticePath+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl).isDirectory()){
					if(conf.get(StrVal.SYS_DEBUG_FLAG).equals(conf.get(StrVal.SYSTEM_SUCCESS_FLAG))){
						log.info(mstNoticePath+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl+" is directory ,directory will ignore.");
					}
					continue;
				}
				if(!jobstatusM.containsKey(ctl)){
					log.info("Mem not exist "+ctl+", put "+conf.get(StrVal.SYSTEM_STATUS_PENDING)+" status to Mem.");
					Map map = JobInfo.readCTLFile(conf,mstNoticePath+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl);
					map.put(conf.get(StrVal.SYSTEM_PARAMETER_STATUS), conf.get(StrVal.SYSTEM_STATUS_PENDING));
					jobstatusM.put(ctl,map);
				}else{ 
					if(conf.get(StrVal.SYS_DEBUG_FLAG).equals(conf.get(StrVal.SYSTEM_SUCCESS_FLAG))){
						log.info("Mem exists control "+ctl+".");
					}
				}
			}
			//lock
			if(conf.get(StrVal.SYS_DEBUG_FLAG).equals(conf.get(StrVal.SYSTEM_SUCCESS_FLAG))){
				log.info("Check directory "+mstLockPath+".");
			}
			String[] mstlock = new File(mstLockPath).list();
			for(int i=0;i<mstlock.length;i++){
				String ctl = mstlock[i];
				if(new File(mstLockPath+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl).isDirectory()){
					if(conf.get(StrVal.SYS_DEBUG_FLAG).equals(conf.get(StrVal.SYSTEM_SUCCESS_FLAG))){
						log.info(mstLockPath+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl+" is directory ,directory will ignore.");
					}
					continue;
				}
				if(!jobstatusM.containsKey(ctl)){
					log.info("Mem not exist "+ctl+", put "+conf.get(StrVal.SYSTEM_STATUS_SUBMIT)+" status to Mem.");
					Map map = JobInfo.readCTLFile(conf,mstLockPath+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl);
					map.put(conf.get(StrVal.SYSTEM_PARAMETER_STATUS), conf.get(StrVal.SYSTEM_STATUS_SUBMIT));
					jobstatusM.put(ctl,map);
				}else{ 
					if(conf.get(StrVal.SYS_DEBUG_FLAG).equals(conf.get(StrVal.SYSTEM_SUCCESS_FLAG))){
						log.info("Mem exists control "+ctl+".");
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

	public int resourceVal(String slave,Map map,double cpuwt,double memwt){
		int cpu = 0;
		int mem = 0;
		int proc = 0;
		int max = 0;
		int res = 100;
		if(!map.containsKey(conf.get(StrVal.SYSTEM_PARAMETER_CPUPNT))){
			log.info(slave+" Lost CPU info.");
			return maxres;
		}
		//
		cpu = Integer.parseInt((String)map.get(conf.get(StrVal.SYSTEM_PARAMETER_CPUPNT)));
		mem = Integer.parseInt((String)map.get(conf.get(StrVal.SYSTEM_PARAMETER_MEMPNT)));
		proc = Integer.parseInt((String)map.get(conf.get(StrVal.SYSTEM_PARAMETER_PRCCNT)));
		double curproc=proc+1;
        if(prclmt<=curproc){
        	log.info(slave+" "+prclmt+"=>"+curproc+" Process reach limit. wait for next time!");
			return maxres;
		}
        double curcpu = cpu+(cpulmt*1.00)/prclmt*cpuwt;
        if(cpulmt<=curcpu){
        	log.info(slave+" "+cpulmt+"=>"+curcpu+" CPU reach limit. wait for next time!");
			return maxres;
		}
        double curmem = mem+(memlmt*1.00)/prclmt*memwt;
        if(memlmt<=curmem){
        	log.info(slave+" "+memlmt+"=>"+curmem+" MEM reach limit. wait for next time!");
			return maxres;
		}
        res = (int) (curcpu+curmem+curproc);
        return res;
	}
	
	public void mvNotice2Lock(String ctl,String jobnode){
		String slvPath = conf.get(StrVal.SYS_BATCH_PATH)+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_INSTANCE)+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_SLAVE);
		String mstPath = conf.get(StrVal.SYS_BATCH_PATH)+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_INSTANCE)+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_MASTER);
		String mstlockPath = mstPath+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_MASTER_LOCK);
		String mstnoticePath = mstPath+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_NOTICE);
		String mstStatusPath = mstPath+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_STATUS);
		Map jobserverinfo = null;
		
		long  starttime = System.currentTimeMillis();
		jobserverinfo = new HashMap();
		jobserverinfo.put(conf.get(StrVal.SYSTEM_PARAMETER_NODE), jobnode);
		jobserverinfo.put(conf.get(StrVal.SYSTEM_PARAMETER_STARTTIME), starttime);
		jobserverinfo.put(conf.get(StrVal.SYSTEM_PARAMETER_ENDTIME), starttime);
		//
		JobInfo.writeCTLFile(conf.get(StrVal.SYSTEM_PARAMETER_NODE)+conf.get(StrVal.SYSTEM_KEYVAL_DELIMITER)+jobnode+conf.get(StrVal.SYSTEM_ROW_DELIMITER), mstnoticePath+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl);
		JobInfo.writeCTLFile(conf.get(StrVal.SYSTEM_PARAMETER_STARTTIME)+conf.get(StrVal.SYSTEM_KEYVAL_DELIMITER)+starttime+conf.get(StrVal.SYSTEM_ROW_DELIMITER), mstnoticePath+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl);
		JobInfo.writeCTLFile(conf.get(StrVal.SYSTEM_PARAMETER_ENDTIME)+conf.get(StrVal.SYSTEM_KEYVAL_DELIMITER)+starttime+conf.get(StrVal.SYSTEM_ROW_DELIMITER), mstnoticePath+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl);
		JobInfo.writeCTLFile(conf.get(StrVal.SYSTEM_PARAMETER_SEQUENCE)+conf.get(StrVal.SYSTEM_KEYVAL_DELIMITER)+starttime+conf.get(StrVal.SYSTEM_ROW_DELIMITER), mstnoticePath+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl);
		//
		jobserverMaping.put(ctl, jobserverinfo);
		Map tmpjobinfo = (Map) jobstatusM.get(ctl);
		log.info(ctl+" update status from "+conf.get(StrVal.SYSTEM_STATUS_PENDING)+" to "+conf.get(StrVal.SYSTEM_STATUS_SUBMIT));
		tmpjobinfo.put(conf.get(StrVal.SYSTEM_PARAMETER_STATUS), conf.get(StrVal.SYSTEM_STATUS_SUBMIT));
		JobInfo.writeCTLFile(conf.get(StrVal.SYSTEM_PARAMETER_STATUS)+conf.get(StrVal.SYSTEM_KEYVAL_DELIMITER)+conf.get(StrVal.SYSTEM_STATUS_SUBMIT)+conf.get(StrVal.SYSTEM_ROW_DELIMITER), mstnoticePath+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl);
		new File(mstnoticePath+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl).renameTo(new File(mstlockPath+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl));
		log.info("Move "+ctl+" from "+mstnoticePath+" to "+mstlockPath+".");
	}
}
