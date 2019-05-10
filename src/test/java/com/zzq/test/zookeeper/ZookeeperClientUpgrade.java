package com.zzq.test.zookeeper;

import com.alibaba.fastjson.JSON;
import com.zzq.test.zookeeper.bean.ZNodeBean;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.io.*;
import java.util.List;
import java.util.Map;

/**
 * @author Zhou Zhong Qing
 * @Title: ${file_name}
 * @Package ${package_name}
 * @Description: java api 永久监听
 * @date 2019/5/7 10:04
 */
public class ZookeeperClientUpgrade implements Watcher{


    private ZooKeeper zooKeeper;

    private ConfigureContainer configureContainer;


    public ZookeeperClientUpgrade(ConfigureContainer configureContainer) {
        this.configureContainer = configureContainer;
    }



    @Override
    public void process(WatchedEvent event) {
        try {

            if (event.getState() == Event.KeeperState.SyncConnected) {
                //System.out.println("watcher received event");
            }
            if (Event.EventType.None == event.getType()) {
                System.out.println("连接建立...");
            }
            System.out.println("回调watcher1实例： 路径" + event.getPath() + " 类型：" + event.getType());
            // 事件类型，状态，和检测的路径
            Event.EventType eventType = event.getType();
            Event.KeeperState state = event.getState();
            String watchPath = event.getPath();
            switch (eventType) {
                case None:
                    break;
                case NodeDeleted:
                    exists(event.getPath(), true);
                    configureContainer.getConfig().remove(event.getPath());
                    break;
                case NodeCreated:
                    exists(event.getPath(), true);
                    configureContainer.getConfig().put(event.getPath(), new String(getData(event.getPath()), "UTF-8"));
                    break;
                case NodeDataChanged:
                    exists(event.getPath(), true);
                    configureContainer.getConfig().put(event.getPath(), new String(getData(event.getPath()), "UTF-8"));
                    break;
                case NodeChildrenChanged:
                    try {
                        //处理收到的消息
                        handleMessage(watchPath);
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                    } catch (KeeperException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    break;
                default:
                    break;
            }

            for (Map.Entry<String, String> entry : configureContainer.getConfig().entrySet()) {
                System.out.println(" key: " + entry.getKey() + " value :" + entry.getValue());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }




    public void handleMessage(String watchPath) throws KeeperException, InterruptedException, UnsupportedEncodingException {
        System.out.println("收到消息");
        //再监听该子节点
        List<String> children = getChildren(watchPath);
        for (String a : children) {
            String childrenPath = watchPath + "/" + a;
            byte[] recivedata = getData(childrenPath);
            String recString = new String(recivedata, "UTF-8");
            System.out.println("receive the path:" + childrenPath + ":data:" + recString);
        }
    }

    public List<String> getChildren(String path) throws KeeperException, InterruptedException {
        //监听该节点子节点的变化情况
        return zooKeeper.getChildren(path, this);
    }


    /***
     * 初始化
     * */
    public void initialize() {
        try {
            zooKeeper = new ZooKeeper("localhost:2181", 20000, this);
            configureContainer.setZooKeeper(zooKeeper);
            doLoadZNodeData();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    /***
     * 载入初始数据
     * 2019年5月9日16:39:40
     * zhouzhongqing
     * */
    private void doLoadZNodeData() {
        //jar 包中 getResourceAsStream可以正确读取
        InputStream inputStream = ZookeeperClientUpgrade.class.getResourceAsStream("/znode.json");
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        StringBuffer jsonBuf = new StringBuffer();
        String line = null;
        try {

            while ((line = reader.readLine()) != null) {
                jsonBuf.append(line + "\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                reader.close();
                inputStream.close();
            } catch (IOException e) {
                e.printStackTrace();

            }
        }


        List<ZNodeBean> zNodeBeanList = JSON.parseArray(jsonBuf.toString(), ZNodeBean.class);
        zNodeBeanList.forEach((k) -> {
            try {
                if (null == exists(k.getPath(), true)) {
                    //节点不存在 则新增
                    System.out.println("新增节点: "+k.getPath());
                    create(k.getPath(),k.getValue().getBytes(),ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });


    }


    public ZooKeeper getZooKeeper() {
        return zooKeeper;
    }


    public Stat exists(String path, boolean watch) throws Exception {
        return zooKeeper.exists(path, watch);
    }

   /* public void handleMessage(String watchPath) throws  Exception {
        System.out.println("收到消息");
        //再监听该子节点
        List<String> Children = getChildren(watchPath);
        for (String a : Children) {
            String childrenpath = watchPath + "/" + a;
            byte[] recivedata = getData(childrenpath);
            String recString = new String(recivedata, "UTF-8");
            System.out.println("receive the path:" + childrenpath + ":data:" + recString);
            //做完了之后，删除该节点
            deletNode(childrenpath, -1);
        }
    }*/

 /*   public List<String> getChildren(String path) throws Exception  {
        //监听该节点子节点的变化情况
        return zooKeeper.getChildren(path, this);
    }*/

    public Stat setData(String path, byte[] data, int version) throws KeeperException, InterruptedException {
        return zooKeeper.setData(path, data, version);
    }

    public void deletNode(String path, int version) throws KeeperException, InterruptedException {
        zooKeeper.delete(path, version);
    }

    public byte[] getData(String path) throws KeeperException, InterruptedException {
        return zooKeeper.getData(path, true, null);
    }

    public String create(String path, byte[] data, List<ACL> acl, CreateMode createMode) throws Exception {
        Stat stat = zooKeeper.exists(path, true);
       /* if(null != stat){
            deletNode(path,-1);
        }*/
        /*
        CreateMode类型分为4种

        1.PERSISTENT--持久型

        2.PERSISTENT_SEQUENTIAL--持久顺序型

        3.EPHEMERAL--临时型

        4.EPHEMERAL_SEQUENTIAL--临时顺序型

        1、2种类型客户端断开后不会消失

        3、4种类型客户端断开后超时时间内没有新的连接节点将会消失
        * */
        return zooKeeper.create(path, data, acl,
                createMode);
    }

}