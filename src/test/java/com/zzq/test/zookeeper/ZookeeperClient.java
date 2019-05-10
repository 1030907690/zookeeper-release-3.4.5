package com.zzq.test.zookeeper;

import org.apache.zookeeper.*;

/**
 * @author Zhou Zhong Qing
 * @Title: ${file_name}
 * @Package ${package_name}
 * @Description: java api
 * @date 2019/5/7 10:04
 */
public class ZookeeperClient {


    /*
     *   watch机制官方说明：一个Watch事件是一个一次性的触发器，当被设置了Watch的数据发生了改变的时候，则服务器将这个改变发送给设置了Watch的客户端，以便通知它们。
    *  可以注册watcher的方法：getData、exists、getChildren。
    * */


    public void initialize(){
        try {
            // 创建一个与服务器的连接
            ZooKeeper zk = new ZooKeeper("localhost:2181",
                    20000, new Watcher() {
                // 监控所有被触发的事件
                @Override
                public void process(WatchedEvent event) {
                        System.out.println("已经触发了" + event.getType() + "事件！" + event.toString() + " path: " + event.getPath() + " getWrapper : "+ event.getWrapper() );

                }
            });

            // 创建一个目录节点
            zk.create("/testRootPath", "testRootData".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);

            // 创建一个子目录节点
            zk.create("/testRootPath/testChildPathOne", "testChildDataOne".getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
            //System.out.println(new String(zk.getData("/testRootPath",false,null)));
            // 取出子目录节点列表
            System.out.println(zk.getChildren("/testRootPath",true));
            // 修改子目录节点数据
            zk.setData("/testRootPath/testChildPathOne","modifyChildDataOne".getBytes(),-1);
            System.out.println("目录节点状态：["+zk.exists("/testRootPath",true)+"]");
            // 创建另外一个子目录节点
            zk.create("/testRootPath/testChildPathTwo", "testChildDataTwo".getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
            System.out.println(new String(zk.getData("/testRootPath/testChildPathTwo",true,null)));
            // 删除子目录节点
            zk.delete("/testRootPath/testChildPathTwo",-1);
            zk.delete("/testRootPath/testChildPathOne",-1);
            // 删除父目录节点
            zk.delete("/testRootPath",-1);
            // 关闭连接
            zk.close();
        }catch (Exception e){
            e.printStackTrace();
        }

}


}