package com.miskss.queue.limit;

import java.lang.instrument.Instrumentation;
import java.util.Collection;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * source: https://github.com/apache/dubbo/pull/9722/files
 * 防止 {@link  LinkedBlockingQueue} 由于无限长度导致 OOM
 * 通过限制Queue的使用内存大小来防止OOM的发生
 * 原理：
 * 主要通过{@link MemoryLimiter} 来限制使用内存
 * 1. 入列前通过{@link MemoryLimiter#acquireInterruptibly(Object)}来检查是否已达到内存最大值，
 * 之后在添加到队列。
 * 2. 出列后扣减内存占用
 *
 * 个人觉得问题点：
 * 1. {@link MemoryLimiter#releaseInterruptibly(Object)} 扣减内存不需要加锁，
 * 因为内存的释放是在对象出列之后，是必然要执行的操作，且{@link MemoryLimiter#getCurrentMemory()} 是{@link java.util.concurrent.atomic.LongAdder} 类型
 * 加减都是线程安全的；
 * 2. 队列的内存占用可能会超过最大值限制，当对象入列之后，再次修改由可能会导致对象内存占用变大。
 *
 * @author peter
 * 2022/08/10 10:09
 */
public class MemoryLimitedLinkedBlockingQueue<E> extends LinkedBlockingQueue<E> {

    private static final long serialVersionUID = 1374792064759926198L;

    private final MemoryLimiter memoryLimiter;

    public MemoryLimitedLinkedBlockingQueue(Instrumentation inst) {
        this(Integer.MAX_VALUE, inst);
    }

    public MemoryLimitedLinkedBlockingQueue(long memoryLimit, Instrumentation inst) {
        super(Integer.MAX_VALUE);
        this.memoryLimiter = new MemoryLimiter(memoryLimit, inst);
    }

    public MemoryLimitedLinkedBlockingQueue(Collection<? extends E> c, long memoryLimit, Instrumentation inst) {
        super(c);
        this.memoryLimiter = new MemoryLimiter(memoryLimit, inst);
    }

    public void setMemoryLimit(long memoryLimit) {
        memoryLimiter.setMemoryLimit(memoryLimit);
    }

    public long getMemoryLimit() {
        return memoryLimiter.getMemoryLimit();
    }

    public long getCurrentMemory() {
        return memoryLimiter.getCurrentMemory();
    }

    public long getCurrentRemainMemory() {
        return memoryLimiter.getCurrentRemainMemory();
    }

    @Override
    public void put(E e) throws InterruptedException {
        memoryLimiter.acquireInterruptibly(e);
        super.put(e);
    }

    @Override
    public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
        return memoryLimiter.acquire(e, timeout, unit) && super.offer(e, timeout, unit);
    }

    @Override
    public boolean offer(E e) {
        return memoryLimiter.acquire(e) && super.offer(e);
    }

    @Override
    public E take() throws InterruptedException {
        final E e = super.take();
        memoryLimiter.releaseInterruptibly(e);
        return e;
    }

    @Override
    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        final E e = super.poll(timeout, unit);
        memoryLimiter.releaseInterruptibly(e, timeout, unit);
        return e;
    }

    @Override
    public E poll() {
        final E e = super.poll();
        memoryLimiter.release(e);
        return e;
    }

    @Override
    public boolean remove(Object o) {
        final boolean success = super.remove(o);
        if (success) {
            memoryLimiter.release(o);
        }
        return success;
    }

    @Override
    public void clear() {
        super.clear();
        memoryLimiter.clear();
    }
}