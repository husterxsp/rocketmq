/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.example.transaction;

import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class TransactionListenerImpl implements TransactionListener {
    private AtomicInteger transactionIndex = new AtomicInteger(0);
    private AtomicInteger checkTimes = new AtomicInteger(0);

    private ConcurrentHashMap<String, Integer> localTrans = new ConcurrentHashMap<>();

    /**
     * 执行本地事务
     *
     * @param msg Half(prepare) message
     * @param arg Custom business parameter
     * @return
     */
    @Override
    public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
        int value = transactionIndex.getAndIncrement();
        int status = value % 3;

        // 假设3条消息的本地事务结果分别为 0，1，2，0，1，2，0，1，2，0
        localTrans.put(msg.getTransactionId(), status);

        System.out.println("executeLocalTransaction:" + msg.getKeys() + ",excute state:" + status + ",current time：" + new Date());
        // UNKNOW 模拟执行本地事务突然宕机的情况（或者本地执行成功发送确认消息失败的场景）
        return LocalTransactionState.UNKNOW;
    }

    /**
     * 检查本地事务的状态
     *
     * @param msg Check message
     * @return
     */
    @Override
    public LocalTransactionState checkLocalTransaction(MessageExt msg) {
        Integer status = localTrans.get(msg.getTransactionId());
        if (null != status) {
            switch (status) {
                case 0:
                    System.out.println(" check result：unknow ，回查次数：" + checkTimes.incrementAndGet());
                    // 依然无法确定本地事务的执行结果，返回unknow，下次会继续回查结果
                    return LocalTransactionState.UNKNOW;
                case 1:
                    System.out.println(" check result：commit message");

                    // 查到本地事务执行成功，返回COMMIT_MESSAGE，producer继续发送确认消息（此逻辑无需自己写，mq本身提供）
                    return LocalTransactionState.COMMIT_MESSAGE;
                case 2:
                    System.out.println(" check result：rollback message");

                    // 查询到本地事务执行失败，需要回滚消息。
                    // 回滚消息，应该是要把消息丢弃。。
                    return LocalTransactionState.ROLLBACK_MESSAGE;
                default:
                    return LocalTransactionState.COMMIT_MESSAGE;
            }
        }
        return LocalTransactionState.COMMIT_MESSAGE;
    }
}
