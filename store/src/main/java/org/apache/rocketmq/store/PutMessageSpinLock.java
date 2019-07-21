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
package org.apache.rocketmq.store;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Spin lock Implementation to put message, suggest using this with low race conditions
 */
public class PutMessageSpinLock implements PutMessageLock {
    //true: Can lock, false : in lock.
    // 注意这里 AtomicBoolean 其实是用了 volatile 实现
    // 为什么用 AtomicBoolean ? emmm。。感觉直接用volatile + unsafe是一样的
    // 那么为什么用volatile？保证可见性，如果不用volatile，那么while循环可能不会及时终止，
    // 还有可能死循环。
    private AtomicBoolean putMessageSpinLock = new AtomicBoolean(true);

    @Override
    public void lock() {
        boolean flag;
        do {
            flag = this.putMessageSpinLock.compareAndSet(true, false);
        }
        while (!flag);
    }

    @Override
    public void unlock() {
        this.putMessageSpinLock.compareAndSet(false, true);
    }
}
