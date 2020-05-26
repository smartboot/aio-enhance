package org.smartboot.aio;

import java.io.IOException;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.AsynchronousChannelProvider;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author 三刀
 * @version V1.0 , 2020/5/25
 */
public class EnhanceAsynchronousChannelProvider extends AsynchronousChannelProvider {
    @Override
    public AsynchronousChannelGroup openAsynchronousChannelGroup(int nThreads, ThreadFactory threadFactory) throws IOException {
        return new EnhanceAsynchronousChannelGroup(this, new ThreadPoolExecutor(nThreads, nThreads,
                0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<Runnable>(nThreads),
                threadFactory), nThreads);
    }

    @Override
    public AsynchronousChannelGroup openAsynchronousChannelGroup(ExecutorService executor, int initialSize) throws IOException {
        return new EnhanceAsynchronousChannelGroup(this, executor, initialSize);
    }

    @Override
    public AsynchronousServerSocketChannel openAsynchronousServerSocketChannel(AsynchronousChannelGroup group) throws IOException {
        return new EnhanceAsynchronousServerSocketChannel(checkAndGet(group));
    }

    @Override
    public AsynchronousSocketChannel openAsynchronousSocketChannel(AsynchronousChannelGroup group) throws IOException {
        return new EnhanceAsynchronousSocketChannel(checkAndGet(group), SocketChannel.open());
    }

    private EnhanceAsynchronousChannelGroup checkAndGet(AsynchronousChannelGroup group) {
        if (!(group instanceof EnhanceAsynchronousChannelGroup)) {
            throw new RuntimeException("invalid class");
        }
        return (EnhanceAsynchronousChannelGroup) group;
    }
}
