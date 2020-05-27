/*******************************************************************************
 * Copyright (c) 2017-2020, org.smartboot. All rights reserved.
 * project name: smart-socket
 * file name: SimilarlyAsynchronousSocketChannel.java
 * Date: 2020-05-24
 * Author: sandao (zhengjunweimail@163.com)
 *
 ******************************************************************************/

package org.smartboot.aio;

import java.io.IOException;
import java.net.SocketAddress;
import java.net.SocketOption;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.CompletionHandler;
import java.nio.channels.ConnectionPendingException;
import java.nio.channels.ReadPendingException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritePendingException;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 模拟JDK7的AIO处理方式
 *
 * @author 三刀
 * @version V1.0 , 2018/5/24
 */
class EnhanceAsynchronousSocketChannel extends AsynchronousSocketChannel {
    private final SocketChannel channel;
    private ByteBuffer readBuffer;
    private ByteBuffer writeBuffer;
    private CompletionHandler<Integer, Object> readCompletionHandler;
    private CompletionHandler<Integer, Object> writeCompletionHandler;
    private CompletionHandler<Void, Object> connectCompletionHandler;
    private Object readAttachment;
    private Object writeAttachment;
    private Object connectAttachment;
    private SelectionKey readSelectionKey;
    private SelectionKey writeSelectionKey;
    private boolean writePending;
    private boolean readPending;
    private boolean connectionPending;
    private SocketAddress remote;
    private AtomicInteger readInvoker = new AtomicInteger(0);
    private AtomicInteger writeInvoker = new AtomicInteger(0);
    private EnhanceAsynchronousChannelGroup group;

    public EnhanceAsynchronousSocketChannel(EnhanceAsynchronousChannelGroup group, SocketChannel channel) throws IOException {
        super(group.provider());
        this.group = group;
        this.channel = channel;
        channel.configureBlocking(false);
    }

    @Override
    public void close() throws IOException {
        IOException exception = null;
        try {
            channel.close();
        } catch (IOException e) {
            exception = e;
        }
        if (readSelectionKey != null) {
            readSelectionKey.cancel();
        }
        if (writeSelectionKey != null) {
            writeSelectionKey.channel();
        }
        if (exception != null) {
            throw exception;
        }
    }

    @Override
    public java.nio.channels.AsynchronousSocketChannel bind(SocketAddress local) throws IOException {
        channel.bind(local);
        return this;
    }

    @Override
    public <T> java.nio.channels.AsynchronousSocketChannel setOption(SocketOption<T> name, T value) throws IOException {
        channel.setOption(name, value);
        return this;
    }

    @Override
    public <T> T getOption(SocketOption<T> name) throws IOException {
        return channel.getOption(name);
    }

    @Override
    public Set<SocketOption<?>> supportedOptions() {
        return channel.supportedOptions();
    }

    @Override
    public java.nio.channels.AsynchronousSocketChannel shutdownInput() throws IOException {
        channel.shutdownInput();
        return this;
    }

    @Override
    public java.nio.channels.AsynchronousSocketChannel shutdownOutput() throws IOException {
        channel.shutdownOutput();
        return this;
    }

    @Override
    public SocketAddress getRemoteAddress() throws IOException {
        return channel.getRemoteAddress();
    }

    @Override
    public <A> void connect(SocketAddress remote, A attachment, CompletionHandler<Void, ? super A> handler) {
        if (connectionPending) {
            throw new ConnectionPendingException();
        }
        connectionPending = true;
        this.connectAttachment = attachment;
        this.connectCompletionHandler = (CompletionHandler<Void, Object>) handler;
        this.remote = remote;
        doConnect();
    }

    @Override
    public Future<Void> connect(SocketAddress remote) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <A> void read(ByteBuffer dst, long timeout, TimeUnit unit, A attachment, CompletionHandler<Integer, ? super A> handler) {
        if (readPending) {
            throw new ReadPendingException();
        }
        readPending = true;
        this.readBuffer = dst;
        this.readAttachment = attachment;
        this.readCompletionHandler = (CompletionHandler<Integer, Object>) handler;
        doRead();
    }

    @Override
    public Future<Integer> read(ByteBuffer readBuffer) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <A> void read(ByteBuffer[] dsts, int offset, int length, long timeout, TimeUnit unit, A attachment, CompletionHandler<Long, ? super A> handler) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <A> void write(ByteBuffer src, long timeout, TimeUnit unit, A attachment, CompletionHandler<Integer, ? super A> handler) {
        if (writePending) {
            throw new WritePendingException();
        }
        writePending = true;
        this.writeBuffer = src;
        this.writeAttachment = attachment;
        this.writeCompletionHandler = (CompletionHandler<Integer, Object>) handler;
        doWrite();
    }

    @Override
    public Future<Integer> write(ByteBuffer src) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <A> void write(ByteBuffer[] srcs, int offset, int length, long timeout, TimeUnit unit, A attachment, CompletionHandler<Long, ? super A> handler) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SocketAddress getLocalAddress() throws IOException {
        return channel.getLocalAddress();
    }

    public void doConnect() {
        try {
            if (channel.connect(remote)) {
                channel.finishConnect();
                connectionPending = false;
                connectCompletionHandler.completed(null, connectAttachment);
                connectAttachment = null;
                if (connectionPending && writeSelectionKey != null) {
                    group.removeOps(writeSelectionKey, SelectionKey.OP_CONNECT);
                }
            } else if (writeSelectionKey == null) {
                group.getWriteWorker().addRegister(new WorkerRegister() {
                    @Override
                    public void callback(Selector selector) {
                        try {
                            writeSelectionKey = channel.register(selector, SelectionKey.OP_CONNECT);
                            writeSelectionKey.attach(EnhanceAsynchronousSocketChannel.this);
                        } catch (ClosedChannelException e) {
                            writeCompletionHandler.failed(e, writeAttachment);
                        }
                    }
                });
            } else {
                group.interestOps(writeSelectionKey, SelectionKey.OP_CONNECT);
            }
        } catch (IOException e) {
            connectCompletionHandler.failed(e, connectAttachment);
        }

    }

    public void doRead() {
        try {
            boolean directRead = readInvoker.getAndIncrement() < EnhanceAsynchronousChannelGroup.MAX_INVOKER;

            int totalSize = 0;
            int readSize = 0;
            while (directRead && readBuffer.hasRemaining()) {
                readSize = channel.read(readBuffer);
                if (readSize <= 0) {
                    if (totalSize == 0) {
                        totalSize = readSize;
                    }
                    break;
                }
                totalSize += readSize;
            }
            if (totalSize != 0 || !readBuffer.hasRemaining()) {
                readPending = false;
                readCompletionHandler.completed(totalSize, readAttachment);
                if (!readPending && readSelectionKey != null) {
                    group.removeOps(readSelectionKey, SelectionKey.OP_READ);
                }
            } else if (readSelectionKey == null) {
                readInvoker.set(0);
                group.getReadWorker().addRegister(new WorkerRegister() {
                    @Override
                    public void callback(Selector selector) {
                        try {
                            readSelectionKey = channel.register(selector, SelectionKey.OP_READ);
                            readSelectionKey.attach(EnhanceAsynchronousSocketChannel.this);
                        } catch (ClosedChannelException e) {
                            readCompletionHandler.failed(e, readAttachment);
                        }
                    }
                });
            } else {
                readInvoker.set(0);
                group.interestOps(readSelectionKey, SelectionKey.OP_READ);
            }
        } catch (IOException e) {
            readCompletionHandler.failed(e, readAttachment);
        }
    }

    public void doWrite() {
        try {
            boolean directWrite = writeInvoker.getAndIncrement() < EnhanceAsynchronousChannelGroup.MAX_INVOKER;
            int totalSize = 0;
            int writeSize = 0;
            while (directWrite && writeBuffer.hasRemaining()) {
                writeSize = channel.write(writeBuffer);
                if (writeSize <= 0) {
                    if (totalSize == 0) {
                        totalSize = writeSize;
                    }
                    break;
                }
                totalSize += writeSize;
            }

            if (totalSize > 0 || !writeBuffer.hasRemaining()) {
                writePending = false;
                writeCompletionHandler.completed(totalSize, writeAttachment);
            } else {
                writeInvoker.set(0);
                if (writeSelectionKey == null) {
                    group.getWriteWorker().addRegister(new WorkerRegister() {
                        @Override
                        public void callback(Selector selector) {
                            try {
                                writeSelectionKey = channel.register(selector, SelectionKey.OP_WRITE);
                                writeSelectionKey.attach(EnhanceAsynchronousSocketChannel.this);
                            } catch (ClosedChannelException e) {
                                writeCompletionHandler.failed(e, writeAttachment);
                            }
                        }
                    });
                } else {
                    group.interestOps(writeSelectionKey, SelectionKey.OP_WRITE);
                }
            }
        } catch (IOException e) {
            writeCompletionHandler.failed(e, writeAttachment);
        }
    }

    @Override
    public boolean isOpen() {
        return channel.isOpen();
    }

    public AtomicInteger getReadInvoker() {
        return readInvoker;
    }
}
