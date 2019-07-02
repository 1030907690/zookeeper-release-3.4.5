package com.zzq.test.master;

import org.apache.zookeeper.*;

import java.io.UnsupportedEncodingException;
import java.util.concurrent.CountDownLatch;

/**
 * @author Zhou Zhong Qing
 * @Title: ${file_name}
 * @Package ${package_name}
 * @Description: 主备切换测试
 * @date 2019/7/2 17:24
 */
public class MasterSwitch implements Watcher {

    public final String ROOT_PATH = "/master_slave_switch";

    private ZooKeeper zooKeeper;

    private static final CountDownLatch cdl = new CountDownLatch(1);

    /**
     * 当前状态
     **/
    private volatile String status;

    /**
     * 主
     **/
    private final String MASTER = "MASTER";

    /**
     * 备
     **/
    private final String SLAVE = "SLAVE";

    public static void main(String[] args) {
        MasterSwitch masterSwitch = new MasterSwitch();
        masterSwitch.initialize();
        masterSwitch.createNode();
        masterSwitch.doProcess();
        try {
            cdl.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    /***
     * zhouzhongqing
     * 2019年7月2日17:39:21
     * 处理自己的业务逻辑
     * */
    private void doProcess() {
        new Thread(){
            @Override
            public void run() {
                while (true){
                    //判断当前是master
                    if(MASTER.equals(status)){
                        //TODO 处理自己的业务逻辑
                        System.out.println(" 正在处理业务 "+ System.currentTimeMillis());
                    }else {
                      //  System.out.println(" 不是master 不能处理业务 " + System.currentTimeMillis());
                    }
                }
            }
        }.start();
    }


    /***
     * 初始连接
     * */
    public void initialize() {
        try {
            zooKeeper = new ZooKeeper("127.0.0.1:2181", 2000, this);
        } catch (Exception e) {
            System.out.println(" 创建连接异常 " + e.getMessage() + " " + e.getStackTrace());
        }
    }


    public void createNode(){
        try {
            //是否存在 先监听一下
            zooKeeper.exists(ROOT_PATH,true);
            zooKeeper.create(ROOT_PATH, ROOT_PATH.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            this.status = MASTER;
            System.out.println("创建节点成功 " + ROOT_PATH);
        } catch (Exception e) {
            System.out.println(" 创建节点异常,节点可能已存在 " + e.getMessage() + " " + e.getStackTrace());
            this.status = SLAVE;
        }
    }


    @Override
    public void process(WatchedEvent event) {
        // 事件类型，状态，和检测的路径
        Event.EventType eventType = event.getType();
        System.out.println("操作的路径 :" + event.getPath());
        switch (eventType) {
            case None:
                break;
            case NodeDeleted:
                if(ROOT_PATH.equals(event.getPath())){
                    createNode();
                }
                break;
            case NodeCreated:

                break;
            case NodeDataChanged:

                break;
            case NodeChildrenChanged:

                break;
            default:
                break;
        }

    }
}
