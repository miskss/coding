package com.miskss.queue.limit;

import org.junit.jupiter.api.Test;

import java.lang.instrument.Instrumentation;

import static org.junit.jupiter.api.Assertions.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import net.bytebuddy.agent.ByteBuddyAgent;


/**
 * @author peter
 * 2022/08/10 10:13
 */
class MemoryLimitedLinkedBlockingQueueTest {
    @Test
    public void test() throws Exception {
        ByteBuddyAgent.install();
        final Instrumentation instrumentation = ByteBuddyAgent.getInstrumentation();
        MemoryLimitedLinkedBlockingQueue<Object> queue = new MemoryLimitedLinkedBlockingQueue<>(1, instrumentation);
        //an object needs more than 1 byte of space, so it will fail here
        assertThat(queue.offer(new Object()), is(false));

        //will success
        queue.setMemoryLimit(Integer.MAX_VALUE);
        assertThat(queue.offer(new Object()), is(true));
    }
}