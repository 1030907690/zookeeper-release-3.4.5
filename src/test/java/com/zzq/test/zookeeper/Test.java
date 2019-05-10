package com.zzq.test.zookeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;

import java.util.concurrent.*;

/**
 * @author Zhou Zhong Qing
 * @Title: ${file_name}
 * @Package ${package_name}
 * @Description: ${todo}
 * @date 2019/5/7 10:03
 */
public class Test {

    private static Object lock = new Object();

    private static final CountDownLatch countDownLatch = new CountDownLatch(1);

    public static void main(String[] args) {

        int processors = Runtime.getRuntime().availableProcessors();
        ExecutorService executorService = new ThreadPoolExecutor(processors * 2, processors * 10, 60L, TimeUnit.SECONDS, new LinkedBlockingQueue<>(processors * 100), Executors.defaultThreadFactory(), new ThreadPoolExecutor.CallerRunsPolicy());

        //第一版
        /*
        executorService.execute(new Runnable() {
            @Override
            public void run() {
                new ZookeeperClient().initialize();
                try {
                    System.in.read();
                }catch (Exception e){
                    e.printStackTrace();
                }

             *//*   synchronized (lock) {
                    try {
                        lock.wait();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                lock.notify();*//*
            }
        });*/


        //第二版
        executorService.execute(() -> {
                try {
                    ConfigureContainer configureContainer = new ConfigureContainer();
                    ZookeeperClientUpgrade zk = new ZookeeperClientUpgrade(configureContainer);
                    zk.initialize();
                    //zk.create("/testRootPath", "testRootData".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
                    //zk.getData("/testRootPath");
                    // zk.deletNode("/testRootPath",-1);
                    //System.in.read();
                    countDownLatch.await();
                } catch (Exception e) {
                    e.printStackTrace();
                }

        });
    }
}
