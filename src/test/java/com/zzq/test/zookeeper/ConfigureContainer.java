package com.zzq.test.zookeeper;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/**
 * @author Zhou Zhong Qing
 * @Title: ${file_name}
 * @Package ${package_name}
 * @Description: 配置容器
 * @date 2019/5/7 14:22
 */
public class ConfigureContainer {

    private final Map<String, String> config = new ConcurrentHashMap<>();
    private ZooKeeper zooKeeper;

    public void setZooKeeper(ZooKeeper zooKeeper) {
        this.zooKeeper = zooKeeper;
    }

    public ZooKeeper getZooKeeper() {
        return zooKeeper;
    }

    public Map<String, String> getConfig() {
        return config;
    }

    /***
     * 取配置
     * */
    public Map<String, String> getConfigs(String... items) {
        Map<String, String> result = new HashMap<>();
        if (null != items && items.length > 0) {
            for (String item : items) {
                if (!config.containsKey(item)) {
                    try {
                        //加入watch 便于下次更改的监听
                        Stat stat = zooKeeper.exists(item, true);
                        if (null != stat) {
                            byte[] bytes = zooKeeper.getData(item, true, null);
                            config.put(item, new String(bytes, "UTF-8"));
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                String value = config.getOrDefault(item, null);
                if (null != value && !"".equals(value)) {
                    result.put(item, value);
                }

            }
        }
        return result;
    }
}
