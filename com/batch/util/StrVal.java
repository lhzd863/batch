package com.batch.util;

public class StrVal {
    //system path
	public static final String SYSTEM_VAR_INSTANCE = "system.var.instance";
	public static final String SYSTEM_VAR_MASTER = "system.var.master";
	public static final String SYSTEM_VAR_STREAM = "system.var.stream";
	public static final String SYSTEM_VAR_LIST = "system.var.list";
	public static final String SYSTEM_VAR_STATUS = "system.var.status";
	public static final String SYSTEM_VAR_NOTICE = "system.var.notice";
	public static final String SYSTEM_VAR_SLAVE = "system.var.slave";
	public static final String SYSTEM_VAR_SLAVE_LIST = "system.var.slave.list";
	public static final String SYSTEM_VAR_MASTER_LOCK = "system.var.master.lock";
	public static final String SYSTEM_VAR_FINISH = "system.var.finish";	
	//flag
	public static final String SYSTEM_SUCCESS_FLAG = "system.success.flag";
	public static final String SYSTEM_FAIL_FLAG = "system.fail.flag";
	public static final String SYSTEM_RUNNING_FLAG = "system.running.flag";
	public static final String SYSTEM_CONTROL_DELIMITER = "system.control.delimiter";
	public static final String SYSTEM_CONTROL_DEL = "system.control.del";
	//status
	public static final String SYSTEM_STATUS_RUNNING = "system.status.running";
	public static final String SYSTEM_STATUS_READY = "system.status.ready";
	public static final String SYSTEM_STATUS_FAIL = "system.status.fail";
	public static final String SYSTEM_STATUS_SUCCESS = "system.status.success";
	public static final String SYSTEM_STATUS_PENDING = "system.status.pending";
	public static final String SYSTEM_STATUS_SUBMIT = "system.status.submit";
	//sys
	public static final String SYS_LOG_PATH = "sys.log.path";
	public static final String SYS_BATCH_PATH = "sys.batch.path";
	public static final String SYS_DIR_DELIMITER = "sys.dir.delimiter";
	public static final String SYS_DEBUG_FLAG = "sys.debug.flag";
	public static final String SYS_HEARTBEADT_TIME = "sys.heartbeat.time";
	public static final String SYS_LOG_KEEPPERIOD = "sys.log.keepperiod";
	//parameter
	public static final String SYSTEM_PARAMETER_TRIGGERTYPE = "system.parameter.tiggertype";
	public static final String SYSTEM_PARAMETER_TRIGGERTIME = "system.parameter.tiggertime";
	public static final String SYSTEM_PARAMETER_TXDATE = "system.parameter.txdate";
	public static final String SYSTEM_PARAMETER_PRIORITY = "system.parameter.priority";
	public static final String SYSTEM_PARAMETER_STATUS = "system.parameter.status";
	public static final String SYSTEM_KEYVAL_DELIMITER = "system.keyval.delimiter";
	public static final String SYSTEM_PARAMETER_STARTTIME = "system.parameter.starttime";
	public static final String SYSTEM_PARAMETER_ENDTIME = "system.parameter.endtime";
	public static final String SYSTEM_PARAMETER_NODE = "system.parameter.node";
	public static final String SYSTEM_PARAMETER_SEQUENCE = "system.parameter.sequence";
	public static final String SYSTEM_PARAMETER_CMD = "system.parameter.cmd";
	public static final String SYSTEM_PARAMETER_CPUPNT = "system.parameter.cpupnt";
	public static final String SYSTEM_PARAMETER_MEMPNT = "system.parameter.mempnt";
	public static final String SYSTEM_PARAMETER_PRCCNT = "system.parameter.prccnt";
	public static final String SYSTEM_PARAMETER_TIMESTAMP = "system.parameter.timestamp";
	public static final String SYSTEM_PARAMETER_FORMAT = "system.parameter.format";
	//limit
	public static final String SYS_LIMIT_SLAVE_CPU = "sys.limit.slave.cpu";
	public static final String SYS_LIMIT_SLAVE_MEM = "sys.limit.slave.mem";
	public static final String SYS_LIMIT_SLAVE_PRC = "sys.limit.slave.prc";	
	public static final String SYS_LIMIT_JOB_TIME = "sys.limit.job.time";
	//pref
	public static final String SYSTEM_START_TIME = "system.start.time";
	public static final String SYSTEM_END_TIME = "system.end.time";
	public static final String SYSTEM_ROW_DELIMITER = "system.row.delimiter";
	//
	public static final String SYSTEM_MASTER_DIR_PARAMETER = "system.master.dir.parameter";
	//dir
	public static final String SYS_DIR_SCRIPT = "sys.dir.script";
	public static final String SYS_CMD_DELIMITER = "sys.cmd.delimiter";
	public static final String SYS_SCRIPT_ENDWITH = "sys.script.endwith";
	public static final String SYS_SCRIPT_ENDWITH_CMD = "sys.script.endwith.cmd";
	//
	public static final String SYSTEM_KEYWORK_CTL = "system.keywork.ctl";
	//
	public static final String RESOURCE_CMD_CPU = "resource.cmd.cpu";
	public static final String RESOURCE_CMD_MEM = "resource.cmd.mem";
	//
	public static final String CTL_CONF_JOB_DELIMITER = "ctl.conf.job.delimiter";
	//
	public static final String TRIGGER_JOB_LOG_PATH = "trigger.job.log.path";
	public static final String TRIGGER_JOB_LOG_NAME = "trigger.job.log.name";
	//作业配置保持类型
	public static final String JOB_CONF_TYPE = "job.conf.type";
	//作业配置文件路径
	public static final String JOB_FILE_PATH = "job.file.path";
	//作业配置文件分隔符
	public static final String JOB_FILE_DELIMITER = "job.file.delimiter";
	//数据库配置信息
	public static final String JOB_DB_IP = "job.db.ip";
	public static final String JOB_DB_PORT = "job.db.port";
	public static final String JOB_DB_USR = "job.db.usr";
	public static final String JOB_DB_PASSWD = "job.db.passwd";
	public static final String JOB_DB_DEFAULTDB = "job.db.defaultdb";
	public static final String JOB_DB_TYPE = "job.db.type";
	//sql
	public static final String SQL_SHD_JOB_INFO = "sql.shd.job.info";
	public static final String SQL_SHD_JOB_TRIGGER = "sql.shd.job.trigger";
	public static final String SQL_SHD_JOB_STATUS = "sql.shd.job.status";
	public static final String SQL_SHD_JOB_STEP = "sql.shd.job.step";
	//job 配置信息
	public static final String JOB_ATTR_POOL = "job.attr.pool";
	public static final String JOB_ATTR_SYSTEM = "job.attr.system";
	public static final String JOB_ATTR_JOB = "job.attr.job";
	public static final String JOB_ATTR_NODE = "job.attr.node";
	public static final String JOB_ATTR_JOBTYPE = "job.attr.jobtype";
	public static final String JOB_ATTR_CHECKTIMEWINDOW = "job.attr.checktimewindow";
	public static final String JOB_ATTR_CHECKTIMETRIGGER = "job.attr.checktimetrigger";
	public static final String JOB_ATTR_CHECKCALENDAR = "job.attr.checkcalendar";
	public static final String JOB_ATTR_CHECKLASTSTATUS = "job.attr.checklaststatus";
	public static final String JOB_ATTR_PRIORITY = "job.attr.priority";
	public static final String JOB_ATTR_ENABLE = "job.attr.enable";
	public static final String JOB_ATTR_TRIGGER_TYPE = "job.attr.trigger.type";
	public static final String JOB_ATTR_EXPRESSION = "job.attr.expression";
	public static final String JOB_ATTR_BATCHNUM = "job.attr.batchnum";
	public static final String JOB_ATTR_CMD = "job.attr.cmd";
	
	//配置文件更新时间
	public static final String CONF_REF_TIME = "conf.ref.time";
	//日志分隔符
	public static final String LOG_RANGE_DELIMITER = "log.range.delimiter";
	//SYSTEM NOT CHANGE map key
	public static final String MAP_KEY_POOL = "map.key.pool";
	public static final String MAP_KEY_SYS = "map.key.sys";
	public static final String MAP_KEY_JOB = "map.key.job";
	public static final String MAP_KEY_BATCHNUM = "map.key.batchnum";
	public static final String MAP_KEY_TXDATE = "map.key.txdate";
	public static final String MAP_KEY_STATUS = "map.key.status";
	public static final String MAP_KEY_STREAM = "map.key.stream";
	public static final String MAP_KEY_DEPENDENCY = "map.key.dependency";
	public static final String MAP_KEY_NODE = "map.key.node";
	public static final String MAP_KEY_MAPPING = "map.key.mapping";
	public static final String MAP_KEY_INFO = "map.key.info";
	public static final String MAP_KEY_STEP = "map.key.step";
	public static final String MAP_KEY_TIMEWINDOW = "map.key.timewindow";
	public static final String MAP_KEY_TRIGGER = "map.key.trigger";
	public static final String MAP_KEY_NODESTATUS = "map.key.nodestatus";
	public static final String MAP_KEY_CTL = "map.key.ctl";
	public static final String MAP_KEY_TYPE = "map.key.type";
	public static final String MAP_KEY_EXPRESSION = "map.key.expression";
	public static final String MAP_KEY_RUNNING = "map.key.running";
	public static final String MAP_KEY_FLAG = "map.key.FLAG";
	//static value
	public static final String VAL_CONSTANT_Y = "Y";
	public static final String VAL_CONSTANT_N = "N";
	public static final String VAL_CONSTANT_CRON = "CRON";
	public static final String VAL_CONSTANT_SIMPLE = "SIMPLE";
	public static final String VAL_CONSTANT_FILE = "FILE";
	public static final String VAL_CONSTANT_DATABASE = "DATABASE";
}
