package com.batch.master;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import com.batch.util.Configuration;
import com.batch.util.JobInfo;
import com.batch.util.LoggerUtil;
import com.batch.util.StrVal;

public class SaveJobStatus implements Runnable{

	Configuration conf = null;
	Logger log = null;
	Map shareM = null;
	Map jobstatusM = null;
    Map jobsteamM = null;
    Map pendingM = null;
    Map serverstatusM = null;
    String cfg = "";
    final static String clss = "save";
    Logger statuslog = null;
    
    //database 
    Connection conn = null;
	PreparedStatement pst=null;
	
	SimpleDateFormat dateformat = new SimpleDateFormat("yyyyMMdd");
	SimpleDateFormat timestampformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    public void initialize(String node,String cfgfile,Map shareM){
    	//init conf
    	this.cfg = cfgfile;
		this.conf = new Configuration();
		conf.initialize(new File(cfg));
		DefaultConf.initialize(conf);
		
    	this.shareM = shareM;
    	this.jobstatusM = (Map) this.shareM.get(StrVal.MAP_KEY_STATUS);
    	this.serverstatusM = (Map) this.shareM.get(StrVal.MAP_KEY_NODESTATUS);
    	
    	SimpleDateFormat time = new SimpleDateFormat("yyyyMMdd");
		Date nowTime = new Date();
		String yyyymmdd = time.format(nowTime);
		if(log==null){
			log = new LoggerUtil().getLoggerByName(conf.get("sys.log.path"), clss+"_"+yyyymmdd);
		}
		
	}

	@Override
	public void run() {
		long starttime = System.currentTimeMillis();
		//
		String mstPath = conf.get(StrVal.SYS_BATCH_PATH)+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_INSTANCE)+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_MASTER);
		String slvPath = conf.get(StrVal.SYS_BATCH_PATH)+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_INSTANCE)+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_SLAVE);
		String mstLockPath = mstPath+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_MASTER_LOCK);
//		String mstStatusPath = mstPath+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_STATUS);
		String resFinishPath = conf.get(StrVal.SYS_BATCH_PATH) +conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_INSTANCE) +conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_FINISH);
		long subvalconf = Long.parseLong(conf.get(StrVal.CONF_REF_TIME));
		boolean flag = true;
		
		while(true){
			//init log
			SimpleDateFormat time = new SimpleDateFormat("yyyyMMdd");
			Date nowTime = new Date();
			String yyyymmdd = time.format(nowTime);
			log = new LoggerUtil().getLoggerByName(conf.get("sys.log.path"), clss+"_"+yyyymmdd);
			//refresh config
			long endtime = System.currentTimeMillis();
			long subval = endtime - starttime;
			
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
				mstPath = conf.get(StrVal.SYS_BATCH_PATH)+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_INSTANCE)+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_MASTER);
				slvPath = conf.get(StrVal.SYS_BATCH_PATH)+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_INSTANCE)+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_SLAVE);
				mstLockPath = mstPath+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_MASTER_LOCK);
//				mstStatusPath = mstPath+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_STATUS);
				resFinishPath = conf.get(StrVal.SYS_BATCH_PATH) +conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_INSTANCE) +conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_FINISH);
				subvalconf = Long.parseLong(conf.get(StrVal.CONF_REF_TIME));
			}
			//
			if(!(conf.get(StrVal.SYSTEM_RUNNING_FLAG).equals("0"))){
				log.info("Stream job exit running flag is "+conf.get(StrVal.SYSTEM_RUNNING_FLAG)+".");
				break;
			}
			if(conn==null){
				conn = JobInfo.getConnectionDB(conf);
				try {
					pst = conn.prepareStatement(conf.get(StrVal.SQL_SHD_JOB_STATUS),ResultSet.TYPE_SCROLL_SENSITIVE,ResultSet.CONCUR_READ_ONLY);
				} catch (SQLException e1) {
					e1.printStackTrace();
				};
				log.info("Connection database update job status.");
			}
			
		    List list = new ArrayList();
			//save status
			Iterator iter = jobstatusM.entrySet().iterator();
			while(iter.hasNext()){
				Entry entry = (Entry) iter.next();
				String key = (String) entry.getKey();
				Map valMap = (Map) entry.getValue();
				String jobstatus = (String) valMap.get(conf.get(StrVal.SYSTEM_PARAMETER_STATUS));
                if(jobstatus.equals(conf.get(StrVal.SYSTEM_STATUS_SUCCESS))||jobstatus.equals(conf.get(StrVal.SYSTEM_STATUS_FAIL))){
                	String[] arrjob = key.split(conf.get(StrVal.SYSTEM_CONTROL_DELIMITER));
                	String batchNum = arrjob[0];
					String batchNumPath = resFinishPath+conf.get(StrVal.SYS_DIR_DELIMITER)+batchNum;
					if(!new File(batchNumPath).exists()){
					   new File(resFinishPath+conf.get(StrVal.SYS_DIR_DELIMITER)+arrjob[0]).mkdirs();
					}
					log.info("Move "+key+" from "+mstLockPath+" to "+batchNumPath+".");
					if(new File(batchNumPath+conf.get(StrVal.SYS_DIR_DELIMITER)+key).exists()){
						log.info(clss+" "+batchNumPath+" exists ctl delete "+key+".");
						new File(batchNumPath+conf.get(StrVal.SYS_DIR_DELIMITER)+key).delete();
					}
					new File(mstLockPath+conf.get(StrVal.SYS_DIR_DELIMITER)+key).renameTo(new File(batchNumPath+conf.get(StrVal.SYS_DIR_DELIMITER)+key));

					//
                	//statuslog = LoggerUtil.getLoggerByName(conf.get("sys.log.path"), "status_"+batchNum);
                	long t_starttime = System.currentTimeMillis();
                	if(valMap.containsKey(conf.get(StrVal.SYSTEM_PARAMETER_STARTTIME))){
                		t_starttime = Long.parseLong((String) valMap.get(conf.get(StrVal.SYSTEM_PARAMETER_STARTTIME)));
                	}
                	long t_endtime = System.currentTimeMillis();
                	if(valMap.containsKey(conf.get(StrVal.SYSTEM_PARAMETER_ENDTIME))){
                		t_endtime = Long.parseLong((String) valMap.get(conf.get(StrVal.SYSTEM_PARAMETER_ENDTIME)));
                	}
                	String t_node = "N/A";
                	if(valMap.containsKey(conf.get(StrVal.SYSTEM_PARAMETER_NODE))){
                		t_node = (String) valMap.get(conf.get(StrVal.SYSTEM_PARAMETER_NODE));
                	}
                	String t_txdate = "N/A";
                	if(valMap.containsKey(conf.get(StrVal.SYSTEM_PARAMETER_TXDATE))){
                		t_txdate = (String) valMap.get(conf.get(StrVal.SYSTEM_PARAMETER_TXDATE));
                	}
                	Map ctlInfoMap = JobInfo.getCTLInfoMap(conf,key);
                	//statuslog.info(key+conf.get(StrVal.LOG_RANGE_DELIMITER)+jobstatus+conf.get(StrVal.LOG_RANGE_DELIMITER)+t_node+conf.get(StrVal.LOG_RANGE_DELIMITER)+t_starttime+conf.get(StrVal.LOG_RANGE_DELIMITER)+t_endtime);
                	log.info("Remove "+key+" from memory.status ["+jobstatus+"].");
                	jobstatusM.remove(key);
                	list.add(key);
                	try {
						pst.setString(1,(String)ctlInfoMap.get(StrVal.MAP_KEY_POOL));
						pst.setString(2,(String)ctlInfoMap.get(StrVal.MAP_KEY_SYS));
						pst.setString(3,(String)ctlInfoMap.get(StrVal.MAP_KEY_JOB));
						pst.setString(4,t_node);
						pst.setString(5,(String)ctlInfoMap.get(StrVal.MAP_KEY_BATCHNUM));
						pst.setString(6,jobstatus);
						pst.setString(7,t_txdate);
						pst.setString(8,timestampformat.format(t_starttime));
						pst.setString(9,timestampformat.format(t_endtime));
						pst.addBatch();
					} catch (SQLException e) {
						e.printStackTrace();
					}
                	//stream 
                }
			}
			if(conf.get(StrVal.SYS_DEBUG_FLAG).equals(conf.get(StrVal.SYSTEM_SUCCESS_FLAG))){
				log.info("Check directory "+mstLockPath+".");
			}
			String[] mstLocklst = new File(mstLockPath).list();
			for(int i=0;i<mstLocklst.length;i++){
				String ctl =  mstLocklst[i];
				//chose server
				Map mstLockCtlMap = JobInfo.readCTLFile(conf,mstLockPath+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl);
				String ctlslvnode = (String) mstLockCtlMap.get(conf.get(StrVal.SYSTEM_PARAMETER_NODE));
				String slvNodeStatusPath = slvPath +conf.get(StrVal.SYS_DIR_DELIMITER)+ ctlslvnode+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_STATUS);
				Map slvNodeSlaveCtlMap = new HashMap();
				if(new File(slvNodeStatusPath).exists()){
					slvNodeSlaveCtlMap = JobInfo.readCTLFile(conf,slvNodeStatusPath+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl);
				}
				if(mstLockCtlMap.containsKey(conf.get(StrVal.SYSTEM_PARAMETER_SEQUENCE))){
					if(mstLockCtlMap.get(conf.get(StrVal.SYSTEM_PARAMETER_STATUS)).equals(conf.get(StrVal.SYSTEM_STATUS_FAIL))||
					   mstLockCtlMap.get(conf.get(StrVal.SYSTEM_PARAMETER_STATUS)).equals(conf.get(StrVal.SYSTEM_STATUS_SUCCESS))){
					   if(conf.get(StrVal.SYS_DEBUG_FLAG).equals(conf.get(StrVal.SYSTEM_SUCCESS_FLAG))){
						  log.info(ctl+" job has finish status "+mstLockCtlMap.get(conf.get(StrVal.SYSTEM_PARAMETER_STATUS))+".");
					   }
					   String[] arrjob = ctl.split(conf.get(StrVal.SYSTEM_CONTROL_DELIMITER));
					   String batchNum = arrjob[0];
					   String batchNumPath = resFinishPath+conf.get(StrVal.SYS_DIR_DELIMITER)+arrjob[0];
					   if(!new File(batchNumPath).exists()){
						  new File(resFinishPath+conf.get(StrVal.SYS_DIR_DELIMITER)+arrjob[0]).mkdirs();
					   }
					   String jobstatus = (String) mstLockCtlMap.get(conf.get(StrVal.SYSTEM_PARAMETER_STATUS));
					   log.info(ctl+" move ctl from "+mstLockPath+" to "+batchNumPath+".");
					   new File(mstLockPath+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl).renameTo(new File(batchNumPath));
					   Map ctlInfoMap = JobInfo.getCTLInfoMap(conf,ctl);
					   //
					   //statuslog = LoggerUtil.getLoggerByName(conf.get("sys.log.path"), "status_"+batchNum);
					    long t_starttime = System.currentTimeMillis();
	                	if(mstLockCtlMap.containsKey(conf.get(StrVal.SYSTEM_PARAMETER_STARTTIME))){
	                		t_starttime = Long.parseLong((String) mstLockCtlMap.get(conf.get(StrVal.SYSTEM_PARAMETER_STARTTIME)));
	                	}
	                	long t_endtime = System.currentTimeMillis();
	                	if(mstLockCtlMap.containsKey(conf.get(StrVal.SYSTEM_PARAMETER_ENDTIME))){
	                		t_endtime = Long.parseLong((String) mstLockCtlMap.get(conf.get(StrVal.SYSTEM_PARAMETER_ENDTIME)));
	                	}
	                	String t_node = "N/A";
	                	if(mstLockCtlMap.containsKey(conf.get(StrVal.SYSTEM_PARAMETER_NODE))){
	                		t_node = (String) mstLockCtlMap.get(conf.get(StrVal.SYSTEM_PARAMETER_NODE));
	                	}
	                	String t_txdate = "N/A";
	                	if(mstLockCtlMap.containsKey(conf.get(StrVal.SYSTEM_PARAMETER_TXDATE))){
	                		t_txdate = (String) mstLockCtlMap.get(conf.get(StrVal.SYSTEM_PARAMETER_TXDATE));
	                	}
	                	//statuslog.info(ctl+conf.get(StrVal.LOG_RANGE_DELIMITER)+jobstatus+conf.get(StrVal.LOG_RANGE_DELIMITER)+t_node+conf.get(StrVal.LOG_RANGE_DELIMITER)+t_starttime+conf.get(StrVal.LOG_RANGE_DELIMITER)+t_endtime);
	                	list.add(ctl);
	                	log.info("Remove "+ctl+" from memory.status ["+jobstatus+"].");
	                	jobstatusM.remove(ctl);
	                	try {
							pst.setString(1,(String)ctlInfoMap.get(StrVal.MAP_KEY_POOL));
							pst.setString(2,(String)ctlInfoMap.get(StrVal.MAP_KEY_SYS));
							pst.setString(3,(String)ctlInfoMap.get(StrVal.MAP_KEY_JOB));
							pst.setString(4,t_node);
							pst.setString(5,(String)ctlInfoMap.get(StrVal.MAP_KEY_BATCHNUM));
							pst.setString(6,jobstatus);
							pst.setString(7,t_txdate);
							pst.setString(8,timestampformat.format(t_starttime));
							pst.setString(9,timestampformat.format(t_endtime));
							pst.addBatch();
						} catch (SQLException e) {
							e.printStackTrace();
						}
	                	//stream
					}
				}else{
					Map ctlInfoMap = JobInfo.getCTLInfoMap(conf,ctl);
					list.add(ctl);
					String[] arrjob = ctl.split(conf.get(StrVal.SYSTEM_CONTROL_DELIMITER));
					String batchNum = arrjob[0];
					String batchNumPath = resFinishPath+conf.get(StrVal.SYS_DIR_DELIMITER)+arrjob[0];
					if(!new File(batchNumPath).exists()){
					   new File(resFinishPath+conf.get(StrVal.SYS_DIR_DELIMITER)+arrjob[0]).mkdirs();
					}
					String jobstatus = (String) mstLockCtlMap.get(conf.get(StrVal.SYSTEM_PARAMETER_STATUS));
					log.info("Move "+ctl+" from "+mstLockPath+" to "+batchNumPath+".");
					new File(mstLockPath+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl).renameTo(new File(batchNumPath));
                	log.info("Remove "+ctl+" from memory.status ["+(String)mstLockCtlMap.get(conf.get(StrVal.SYSTEM_PARAMETER_STATUS))+"].");
                	jobstatusM.remove(ctl);
                	Date nowTimestart = new Date();
                	SimpleDateFormat yyyymmddfmt = new SimpleDateFormat("yyyyMMdd");
                	try {
						pst.setString(1,(String)ctlInfoMap.get(StrVal.MAP_KEY_POOL));
						pst.setString(2,(String)ctlInfoMap.get(StrVal.MAP_KEY_SYS));
						pst.setString(3,(String)ctlInfoMap.get(StrVal.MAP_KEY_JOB));
						pst.setString(4,(String)mstLockCtlMap.get(conf.get(StrVal.SYSTEM_PARAMETER_NODE)));
						pst.setString(5,(String)ctlInfoMap.get(StrVal.MAP_KEY_BATCHNUM));
						pst.setString(6,(String)mstLockCtlMap.get(conf.get(StrVal.SYSTEM_PARAMETER_STATUS)));
						pst.setString(7,yyyymmddfmt.format(nowTimestart));
						pst.setString(8,timestampformat.format(System.currentTimeMillis()));
						pst.setString(9,timestampformat.format(System.currentTimeMillis()));
						pst.addBatch();
					} catch (SQLException e) {
						e.printStackTrace();
					}
				}
			}
			try {
				if(list.size()>0){
					pst.executeBatch();
					conn.commit();
				}
			} catch (SQLException e1) {
				e1.printStackTrace();
			}finally{
				try {
					pst.close();
				} catch (SQLException e) {
					e.printStackTrace();
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
