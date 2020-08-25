package com.netflix.discovery;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.netflix.appinfo.InstanceInfo;
import com.netflix.discovery.util.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A task for updating and replicating the local instanceinfo to the remote server. Properties of this task are:
 * - configured with a single update thread to guarantee sequential update to the remote server
 * - update tasks can be scheduled on-demand via onDemandUpdate()
 * - task processing is rate limited by burstSize
 * - a new update task is always scheduled automatically after an earlier update task. However if an on-demand task
 *   is started, the scheduled automatic update task is discarded (and a new one will be scheduled after the new
 *   on-demand update).
 *
 *   @author dliu
 */
class InstanceInfoReplicator implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(InstanceInfoReplicator.class);

    private final DiscoveryClient discoveryClient;
    // 应用实例信息
    private final InstanceInfo instanceInfo;
    // 向 Eureka Server 同步服务实例信息的时间间隔，单位为秒，默认 30 秒
    private final int replicationIntervalSeconds;
    // 定时执行器
    private final ScheduledExecutorService scheduler;

    // 定时执行任务的 Future
    // 为什么此处设置 scheduledPeriodicRef ？在 InstanceInfoReplicator#onDemandUpdate() 方法会看到具体用途
    private final AtomicReference<Future> scheduledPeriodicRef;
    // 是否开启调度
    private final AtomicBoolean started;

    // 限流相关，跳过
    private final RateLimiter rateLimiter;
    // 限流相关，跳过
    private final int burstSize;
    // 限流相关，跳过
    private final int allowedRatePerMinute;

    InstanceInfoReplicator(DiscoveryClient discoveryClient, InstanceInfo instanceInfo, int replicationIntervalSeconds, int burstSize) {
        this.discoveryClient = discoveryClient;
        this.instanceInfo = instanceInfo;
        this.scheduler = Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder().setNameFormat("DiscoveryClient-InstanceInfoReplicator-%d").setDaemon(true).build());

        this.scheduledPeriodicRef = new AtomicReference<Future>();

        this.started = new AtomicBoolean(false);
        this.rateLimiter = new RateLimiter(TimeUnit.MINUTES);
        this.replicationIntervalSeconds = replicationIntervalSeconds;
        this.burstSize = burstSize;

        // burstSize 默认为 2
        this.allowedRatePerMinute = 60 * this.burstSize / this.replicationIntervalSeconds;
        logger.info("InstanceInfoReplicator onDemand update allowed rate per min is {}", allowedRatePerMinute);
    }

    public void start(int initialDelayMs) {
        if (started.compareAndSet(false, true)) {
            // 实际作用为开启 run() 方法中的注册逻辑
            // isInstanceInfoDirty = true;
            // lastDirtyTimestamp = System.currentTimeMillis();
            instanceInfo.setIsDirty();  // for initial register
            // 延迟定时任务
            Future next = scheduler.schedule(this, initialDelayMs, TimeUnit.SECONDS);
            // 为什么此处设置 scheduledPeriodicRef ？在 InstanceInfoReplicator#onDemandUpdate() 方法会看到具体用途
            scheduledPeriodicRef.set(next);
        }
    }

    public void stop() {
        shutdownAndAwaitTermination(scheduler);
        started.set(false);
    }

    private void shutdownAndAwaitTermination(ExecutorService pool) {
        pool.shutdown();
        try {
            if (!pool.awaitTermination(3, TimeUnit.SECONDS)) {
                pool.shutdownNow();
            }
        } catch (InterruptedException e) {
            logger.warn("InstanceInfoReplicator stop interrupted");
        }
    }

    // 实际调用处只有 DiscoveryClient#initScheduledTasks 方法
    public boolean onDemandUpdate() {
        // 获取令牌，获取成功，向 Eureka Server 发起注册
        if (rateLimiter.acquire(burstSize, allowedRatePerMinute)) {  // 限流相关
            if (!scheduler.isShutdown()) {
                scheduler.submit(new Runnable() {
                    @Override
                    public void run() {
                        logger.debug("Executing on-demand update of local InstanceInfo");
    
                        Future latestPeriodic = scheduledPeriodicRef.get();
                        if (latestPeriodic != null && !latestPeriodic.isDone()) {
                            logger.debug("Canceling the latest scheduled update, it will be rescheduled at the end of on demand update");
                            // 将先前的任务取消
                            latestPeriodic.cancel(false);
                        }

                        // 又再此处进行手动调用 run 方法，用意是？？？
                        InstanceInfoReplicator.this.run();
                    }
                });
                return true;
            } else {
                logger.warn("Ignoring onDemand update due to stopped scheduler");
                return false;
            }
        } else {
            logger.warn("Ignoring onDemand update due to rate limiter");
            return false;
        }
    }

    public void run() {
        try {
            // 更新服务实例信息
            discoveryClient.refreshInstanceInfo();

            // 在 start 方法中设置了 dirtyTimestamp 时间戳，延迟 40 秒之后，执行到此处开始进行注册
            // start 方法中 instanceInfo.setIsDirty() 的具体操作
            // isInstanceInfoDirty = true;
            // lastDirtyTimestamp = System.currentTimeMillis();
            // isDirtyWithTime 作用就是判断实例信息是否有变更，如果有变更返回实例信息变更时的时间戳
            Long dirtyTimestamp = instanceInfo.isDirtyWithTime();
            if (dirtyTimestamp != null) {
                // 发起注册
                discoveryClient.register();
                instanceInfo.unsetIsDirty(dirtyTimestamp);
            }
        } catch (Throwable t) {
            logger.warn("There was a problem with the instance info replicator", t);
        } finally {
            // 延迟任务
            // replicationIntervalSeconds 为向 Eureka Server 同步服务实例信息的时间间隔，单位为秒，默认 30 秒
            // 此处的含义是，再过 30 秒执行一次 run 方法，其作用是？
            Future next = scheduler.schedule(this, replicationIntervalSeconds, TimeUnit.SECONDS);
            scheduledPeriodicRef.set(next);
        }
    }

}
