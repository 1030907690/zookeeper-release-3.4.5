package com.zzq.test.zookeeper.bean;


import lombok.Getter;
import lombok.Setter;

/**
 * @author Zhou Zhong Qing
 * @Title: ${file_name}
 * @Package ${package_name}
 * @Description: znode
 * @date 2019/5/9 16:40
 */
@Getter
@Setter
public class ZNodeBean {

    private String path;
    private String value;

    private String description;
}
