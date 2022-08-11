package com.miskss.queue.limit;

import java.lang.instrument.Instrumentation;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 内存大小限制器
 *
 * @author peter
 * 2022/08/10 10:10
 */
public class MemoryLimiter {

    /**
     * 用于获取对象的内存大小的工具
     */
    private final Instrumentation inst;

    /**
     * 使用内存的最大值
     */
    private volatile long memoryLimit;
    /**
     * 当前已使用的内存
     */
    private final LongAdder memory = new LongAdder();

    /**
     * 入列的写锁
     */
    private final ReentrantLock acquireLock = new ReentrantLock();

    /**
     * 内存未满的条件。当内存已满 notLimited.await（），当内存未满notLimited.signal()
     */
    private final Condition notLimited = acquireLock.newCondition();

    /**
     * 出列的释放锁
     */
    private final ReentrantLock releaseLock = new ReentrantLock();

    /**
     * 内存已空条件。当内存已空，无法完成扣减 notEmpty.await()，当内存未空时notEmpty.signal()
     */
    private final Condition notEmpty = releaseLock.newCondition();

    public MemoryLimiter(Instrumentation inst) {
        this(Integer.MAX_VALUE, inst);
    }

    public MemoryLimiter(long memoryLimit, Instrumentation inst) {
        if (memoryLimit <= 0) {
            throw new IllegalArgumentException();
        }
        this.memoryLimit = memoryLimit;
        this.inst = inst;
    }

    public void setMemoryLimit(long memoryLimit) {
        if (memoryLimit <= 0) {
            throw new IllegalArgumentException();
        }
        this.memoryLimit = memoryLimit;
    }

    public long getMemoryLimit() {
        return memoryLimit;
    }

    public long getCurrentMemory() {
        return memory.sum();
    }

    public long getCurrentRemainMemory() {
        return getMemoryLimit() - getCurrentMemory();
    }

    private void signalNotEmpty() {
        releaseLock.lock();
        try {
            notEmpty.signal();
        } finally {
            releaseLock.unlock();
        }
    }

    private void signalNotLimited() {
        acquireLock.lock();
        try {
            notLimited.signal();
        } finally {
            acquireLock.unlock();
        }
    }

    /**
     * Locks to prevent both acquires and releases.
     */
    private void fullyLock() {
        acquireLock.lock();
        releaseLock.lock();
    }

    /**
     * Unlocks to allow both acquires and releases.
     */
    private void fullyUnlock() {
        releaseLock.unlock();
        acquireLock.unlock();
    }

    public boolean acquire(Object e) {
        if (e == null) {
            throw new NullPointerException();
        }
        if (memory.sum() >= memoryLimit) {
            return false;
        }
        acquireLock.lock();
        try {
            final long sum = memory.sum();
            final long objectSize = inst.getObjectSize(e);
            if (sum + objectSize >= memoryLimit) {
                return false;
            }
            memory.add(objectSize);
            if (sum < memoryLimit) {
                notLimited.signal();
            }
        } finally {
            acquireLock.unlock();
        }
        if (memory.sum() > 0) {
            signalNotEmpty();
        }
        return true;
    }

    public void acquireInterruptibly(Object e) throws InterruptedException {
        if (e == null) {
            throw new NullPointerException();
        }
        //获取写锁
        acquireLock.lockInterruptibly();
        try {
            //获取入列的对象的大小
            final long objectSize = inst.getObjectSize(e);
            while (memory.sum() >= memoryLimit) {
                //超过最大内存，wait 等待
                notLimited.await();
            }
            memory.add(objectSize);
            if (memory.sum() < memoryLimit) {
                notLimited.signal();
            }
        } finally {
            acquireLock.unlock();
        }
        if (memory.sum() > 0) {
            signalNotEmpty();
        }
    }

    public boolean acquire(Object e, long timeout, TimeUnit unit) throws InterruptedException {
        if (e == null) {
            throw new NullPointerException();
        }
        long nanos = unit.toNanos(timeout);
        acquireLock.lockInterruptibly();
        try {
            final long objectSize = inst.getObjectSize(e);
            while (memory.sum() + objectSize >= memoryLimit) {
                if (nanos <= 0) {
                    return false;
                }
                nanos = notLimited.awaitNanos(nanos);
            }
            memory.add(objectSize);
            if (memory.sum() < memoryLimit) {
                notLimited.signal();
            }
        } finally {
            acquireLock.unlock();
        }
        if (memory.sum() > 0) {
            signalNotEmpty();
        }
        return true;
    }

    public void release(Object e) {
        if (null == e) {
            return;
        }
        if (memory.sum() == 0) {
            return;
        }
        releaseLock.lock();
        try {
            final long objectSize = inst.getObjectSize(e);
            if (memory.sum() > 0) {
                memory.add(-objectSize);
                if (memory.sum() > 0) {
                    notEmpty.signal();
                }
            }
        } finally {
            releaseLock.unlock();
        }
        if (memory.sum() < memoryLimit) {
            signalNotLimited();
        }
    }

    public void releaseInterruptibly(Object e) throws InterruptedException {
        if (null == e) {
            return;
        }
        releaseLock.lockInterruptibly();
        try {
            final long objectSize = inst.getObjectSize(e);
            while (memory.sum() == 0) {
                notEmpty.await();
            }
            memory.add(-objectSize);
            if (memory.sum() > 0) {
                notEmpty.signal();
            }
        } finally {
            releaseLock.unlock();
        }
        if (memory.sum() < memoryLimit) {
            signalNotLimited();
        }
    }

    public void releaseInterruptibly(Object e, long timeout, TimeUnit unit) throws InterruptedException {
        if (null == e) {
            return;
        }
        long nanos = unit.toNanos(timeout);
        releaseLock.lockInterruptibly();
        try {
            final long objectSize = inst.getObjectSize(e);
            while (memory.sum() == 0) {
                if (nanos <= 0) {
                    return;
                }
                nanos = notEmpty.awaitNanos(nanos);
            }
            memory.add(-objectSize);
            if (memory.sum() > 0) {
                notEmpty.signal();
            }
        } finally {
            releaseLock.unlock();
        }
        if (memory.sum() < memoryLimit) {
            signalNotLimited();
        }
    }

    public void clear() {
        fullyLock();
        try {
            if (memory.sumThenReset() < memoryLimit) {
                notLimited.signal();
            }
        } finally {
            fullyUnlock();
        }
    }
}

