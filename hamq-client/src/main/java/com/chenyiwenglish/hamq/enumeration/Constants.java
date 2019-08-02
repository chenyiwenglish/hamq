package com.chenyiwenglish.hamq.enumeration;

public class Constants {
    public static final int FADING_MAX_TIME = 86400000; // 衰减最大延迟, 单位毫秒 (延迟超过一天意义不大)

    public static final int WORK_POOL_CORE_SIZE = 8;
    public static final int WORK_POOL_MAX_SIZE = 20;
    public static final int WORK_POOL_IDLE_TIME = 60000; // 线程池中线程可空闲时间, 单位毫秒

    public static final int SHUTDOWN_MAX_WAIT_TIME = 2000; // 线程或线程池关闭时最大等待时间, 单位毫秒

    public static final String HAMQ_DATA_FILE_PATH_PREFIX = "../data/hamq/"; // ../data/hamq/<queueId>/
    public static final String HAMQ_DATA_FILE_READY_SUFFIX = ".ready";
    public static final String HAMQ_DATA_FILE_SUCCESS_SUFFIX = ".success";
    public static final long HAMQ_DATA_FILE_RESERVE_MAX_TIME = 604800000; // data文件保留的最长时间, 单位毫秒
    public static final int HAMQ_DATA_FILE_CHECK_INTERVAL = 60000; // 检查data文件的间隔, 单位毫秒
    public static final int HAMQ_STATUS_CHECK_INTERVAL = 60000; // 队列状态检查的间隔, 单位毫秒
    public static final int HAMQ_MYSQL_CHECK_INTERVAL = 1000; // mysql中的消息检查间隔, 单位毫秒
}
