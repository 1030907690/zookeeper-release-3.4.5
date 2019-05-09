/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.server.quorum.flexible;

import java.util.HashSet;


/**
 * This class implements a validator for majority quorums. The 
 * implementation is straightforward.
 *  这个类实现了对法定服务器的校验
 */
public class QuorumMaj implements QuorumVerifier {


    // 一半的服务器数量，如果是5，则half=2,只要可用的服务器数量大于2，则整个zk就是可用的
    int half;
    
    /**
     * Defines a majority to avoid computing it every time.
     * 
     * @param n number of servers
     */
    public QuorumMaj(int n){
        this.half = n/2;
    }
    
    /**
     * Returns weight of 1 by default. 权重
     * 
     * @param id 
     */
    @Override
    public long getWeight(long id){
        return (long) 1;
    }
    
    /**
     * Verifies if a set is a majority.
     */
    @Override
    public boolean containsQuorum(HashSet<Long> set){ //传入一组服务器id,校验必须大于半数才能正常提供服务
        return (set.size() > half);
    }
    
}
