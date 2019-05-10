package com.zzq.test.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;

/**
 * @author Zhou Zhong Qing
 * @Title: ${file_name}
 * @Package ${package_name}
 * @Description: ${todo}
 * @date 2019/5/7 14:37
 */
public class ZkWatcher implements Watcher {

    private ConfigureContainer configureContainer;

    public ZkWatcher(ConfigureContainer configureContainer) {
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


    public void exists(String path, boolean watch) throws Exception {
        configureContainer.getZooKeeper().exists(path, watch);
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
        return configureContainer.getZooKeeper().getChildren(path, this);
    }

    public Stat setData(String path, byte[] data, int version) throws KeeperException, InterruptedException {
        return configureContainer.getZooKeeper().setData(path, data, version);
    }

    public void deletNode(String path, int version) throws KeeperException, InterruptedException {
        configureContainer.getZooKeeper().delete(path, version);
    }

    public byte[] getData(String path) throws KeeperException, InterruptedException {
        return configureContainer.getZooKeeper().getData(path, true, null);
    }


}
