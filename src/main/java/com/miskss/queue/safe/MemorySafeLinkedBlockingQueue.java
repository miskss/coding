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

package com.miskss.queue.safe;

import com.miskss.queue.limit.MemoryLimitedLinkedBlockingQueue;

import java.util.Collection;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Can completely solve the OOM problem caused by {@link LinkedBlockingQueue},
 * does not depend on {@link java.lang.instrument.Instrumentation} and is easier to use than
 * {@link MemoryLimitedLinkedBlockingQueue}.
 *
 * @see <a href="https://github.com/apache/incubator-shenyu/blob/master/shenyu-common/src/main/java/org/apache/shenyu/common/concurrent/MemorySafeLinkedBlockingQueue.java">MemorySafeLinkedBlockingQueue</a>
 * source： https://github.com/apache/dubbo/pull/10021
 * 内存安全的阻塞队列，防止 {@link  LinkedBlockingQueue} 由于无限长度导致 OOM
 * <p>
 * 原理：
 * {@link MemoryLimitCalculator#refresh()} 每50ms 会刷新一次当前JVM的可用内存{@link Runtime#freeMemory()}
 * 元素入队时：通过判断JVM 的可用内存是否大于限定的内存，来判定元素是否可以入队
 * <p>
 *  1. 由于存在50ms的刷新间隔且MemorySafeLBQ只关注剩余内存，假设有如下临界条件，
 * 当剩余内存检测还剩257MB，正常是可以插入的，
 * 但是我这次插入一个300MB的对象，
 * 因为MemorySafeLBQ不计算插入对象的大小，那这样不就OOM了吗？
 * <p>
 * 答：这种情况不成立，由于对象在入队之前，一定是已在jvm里面分配了内存空间的。
 * </p>
 * </p>
 */
public class MemorySafeLinkedBlockingQueue<E> extends LinkedBlockingQueue<E> {

    private static final long serialVersionUID = 8032578371739960142L;

    public static int THE_256_MB = 256 * 1024 * 1024;

    private int maxFreeMemory;

    public MemorySafeLinkedBlockingQueue() {
        this(THE_256_MB);
    }

    public MemorySafeLinkedBlockingQueue(final int maxFreeMemory) {
        super(Integer.MAX_VALUE);
        this.maxFreeMemory = maxFreeMemory;
    }

    public MemorySafeLinkedBlockingQueue(final Collection<? extends E> c,
                                         final int maxFreeMemory) {
        super(c);
        this.maxFreeMemory = maxFreeMemory;
    }

    /**
     * set the max free memory.
     *
     * @param maxFreeMemory the max free memory
     */
    public void setMaxFreeMemory(final int maxFreeMemory) {
        this.maxFreeMemory = maxFreeMemory;
    }

    /**
     * get the max free memory.
     *
     * @return the max free memory limit
     */
    public int getMaxFreeMemory() {
        return maxFreeMemory;
    }

    /**
     * determine if there is any remaining free memory.
     *
     * @return true if has free memory
     */
    public boolean hasRemainedMemory() {
        return MemoryLimitCalculator.maxAvailable() > maxFreeMemory;
    }

    @Override
    public void put(final E e) throws InterruptedException {
        if (hasRemainedMemory()) {
            super.put(e);
        }
    }

    @Override
    public boolean offer(final E e, final long timeout, final TimeUnit unit) throws InterruptedException {
        return hasRemainedMemory() && super.offer(e, timeout, unit);
    }

    @Override
    public boolean offer(final E e) {
        return hasRemainedMemory() && super.offer(e);
    }
}
