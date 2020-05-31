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
import java.nio.channels.AlreadyConnectedException;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.CompletionHandler;
import java.nio.channels.ConnectionPendingException;
import java.nio.channels.NotYetConnectedException;
import java.nio.channels.ReadPendingException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ShutdownChannelGroupException;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritePendingException;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * 模拟JDK7的AIO处理方式
 *
 * @author 三刀
 * @version V1.0 , 2018/5/24
 */
class EnhanceAsynchronousSocketChannel extends AsynchronousSocketChannel {
    private final SocketChannel channel;
    private final EnhanceAsynchronousChannelGroup group;
    private final EnhanceAsynchronousChannelGroup.Worker readWorker;
    private final EnhanceAsynchronousChannelGroup.Worker writeWorker;
    private ByteBuffer readBuffer;
    private Scattering readScattering;
    private ByteBuffer writeBuffer;
    private Scattering writeScattering;
    private CompletionHandler<Number, Object> readCompletionHandler;
    private CompletionHandler<Number, Object> writeCompletionHandler;
    private CompletionHandler<Void, Object> connectCompletionHandler;
    private FutureCompletionHandler<Void, Void> connectFuture;
    private FutureCompletionHandler<? extends Number, Object> readFuture;
    private FutureCompletionHandler<? extends Number, Object> writeFuture;
    private Object readAttachment;
    private Object writeAttachment;
    private Object connectAttachment;
    private SelectionKey readSelectionKey;
    private SelectionKey writeSelectionKey;
    private boolean writePending;
    private boolean readPending;
    private boolean connectionPending;
    private SocketAddress remote;

    public EnhanceAsynchronousSocketChannel(EnhanceAsynchronousChannelGroup group, SocketChannel channel) throws IOException {
        super(group.provider());
        this.group = group;
        this.channel = channel;
        readWorker = group.getReadWorker();
        writeWorker = group.getWriteWorker();
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
    public AsynchronousSocketChannel bind(SocketAddress local) throws IOException {
        channel.bind(local);
        return this;
    }

    @Override
    public <T> AsynchronousSocketChannel setOption(SocketOption<T> name, T value) throws IOException {
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
    public AsynchronousSocketChannel shutdownInput() throws IOException {
        channel.shutdownInput();
        return this;
    }

    @Override
    public AsynchronousSocketChannel shutdownOutput() throws IOException {
        channel.shutdownOutput();
        return this;
    }

    @Override
    public SocketAddress getRemoteAddress() throws IOException {
        return channel.getRemoteAddress();
    }

    @Override
    public <A> void connect(SocketAddress remote, A attachment, CompletionHandler<Void, ? super A> handler) {
        if (group.isTerminated()) {
            throw new ShutdownChannelGroupException();
        }
        if (channel.isConnected()) {
            throw new AlreadyConnectedException();
        }
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
        FutureCompletionHandler<Void, Void> connectFuture = new FutureCompletionHandler<>();
        connect(remote, null, connectFuture);
        this.connectFuture = connectFuture;
        return connectFuture;
    }

    @Override
    public <A> void read(ByteBuffer dst, long timeout, TimeUnit unit, A attachment, CompletionHandler<Integer, ? super A> handler) {
        read0(dst, null, timeout, unit, attachment, handler);
    }

    private <V extends Number, A> void read0(ByteBuffer readBuffer, Scattering scattering, long timeout, TimeUnit unit, A attachment, CompletionHandler<V, ? super A> handler) {
        if (!channel.isConnected()) {
            throw new NotYetConnectedException();
        }
        if (readPending) {
            throw new ReadPendingException();
        }
        readPending = true;
        this.readBuffer = readBuffer;
        this.readScattering = scattering;
        this.readAttachment = attachment;
        if (timeout > 0) {
            readFuture = new FutureCompletionHandler<>(readCompletionHandler, readAttachment);
            readCompletionHandler = (CompletionHandler<Number, Object>) readFuture;
            group.getScheduledExecutor().schedule(readFuture, timeout, unit);
        } else {
            this.readCompletionHandler = (CompletionHandler<Number, Object>) handler;
        }
        doRead();
    }

    @Override
    public Future<Integer> read(ByteBuffer readBuffer) {
        FutureCompletionHandler<Integer, Object> readFuture = new FutureCompletionHandler<>();
        read(readBuffer, 0, TimeUnit.MILLISECONDS, null, readFuture);
        this.readFuture = readFuture;
        return readFuture;
    }

    @Override
    public <A> void read(ByteBuffer[] dsts, int offset, int length, long timeout, TimeUnit unit, A attachment, CompletionHandler<Long, ? super A> handler) {
        read0(null, new Scattering(dsts, offset, length), timeout, unit, attachment, handler);
    }

    @Override
    public <A> void write(ByteBuffer src, long timeout, TimeUnit unit, A attachment, CompletionHandler<Integer, ? super A> handler) {
        write0(src, null, timeout, unit, attachment, handler);
    }

    private <V extends Number, A> void write0(ByteBuffer writeBuffer, Scattering scattering, long timeout, TimeUnit unit, A attachment, CompletionHandler<V, ? super A> handler) {
        if (!channel.isConnected()) {
            throw new NotYetConnectedException();
        }
        if (writePending) {
            throw new WritePendingException();
        }

        writePending = true;
        this.writeBuffer = writeBuffer;
        this.writeScattering = scattering;
        this.writeAttachment = attachment;
        if (timeout > 0) {
            writeFuture = new FutureCompletionHandler<>(writeCompletionHandler, writeAttachment);
            writeCompletionHandler = (CompletionHandler<Number, Object>) writeFuture;
            group.getScheduledExecutor().schedule(writeFuture, timeout, unit);
        } else {
            this.writeCompletionHandler = (CompletionHandler<Number, Object>) handler;
        }
        doWrite();
    }

    @Override
    public Future<Integer> write(ByteBuffer src) {
        FutureCompletionHandler<Integer, Object> writeFuture = new FutureCompletionHandler<>();
        write0(src, null, 0, TimeUnit.MILLISECONDS, null, writeFuture);
        this.writeFuture = writeFuture;
        return writeFuture;
    }

    @Override
    public <A> void write(ByteBuffer[] srcs, int offset, int length, long timeout, TimeUnit unit, A attachment, CompletionHandler<Long, ? super A> handler) {
        write0(null, new Scattering(srcs, offset, length), timeout, unit, attachment, handler);
    }

    @Override
    public SocketAddress getLocalAddress() throws IOException {
        return channel.getLocalAddress();
    }

    public void doConnect() {
        try {
            //此前通过Future调用,且触发了cancel
            if (connectFuture != null && connectFuture.isDone()) {
                resetConnect();
                return;
            }
            boolean connected = channel.isConnectionPending();
            if (connected || channel.connect(remote)) {
                connected = channel.finishConnect();
            }
            if (connected) {
                CompletionHandler<Void, Object> completionHandler = connectCompletionHandler;
                Object attach = connectAttachment;
                resetConnect();
                completionHandler.completed(null, attach);
            } else if (writeSelectionKey == null) {
                writeWorker.addRegister(new WorkerRegister() {
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
                throw new IOException("unKnow exception");
            }
        } catch (IOException e) {
            connectCompletionHandler.failed(e, connectAttachment);
        }

    }

    private void resetConnect() {
        connectionPending = false;
        connectFuture = null;
        connectAttachment = null;
        connectCompletionHandler = null;
    }

    public void doRead() {
        try {
            //此前通过Future调用,且触发了cancel
            if (readFuture != null && readFuture.isDone()) {
                group.removeOps(readSelectionKey, SelectionKey.OP_READ);
                resetRead();
                return;
            }

            boolean directRead = Thread.currentThread() == readWorker.getWorkerThread()
                    && readWorker.getInvoker().getAndIncrement() < EnhanceAsynchronousChannelGroup.MAX_INVOKER;

            long totalSize = 0;
            long readSize;
            boolean hasRemain = true;
            while (directRead && hasRemain) {
                if (readScattering != null) {
                    readSize = channel.read(readScattering.getBuffers(), readScattering.getOffset(), readScattering.getLength());
                    hasRemain = hasRemaining(readScattering);
                } else {
                    readSize = channel.read(readBuffer);
                    hasRemain = readBuffer.hasRemaining();
                }
                if (readSize <= 0) {
                    if (totalSize == 0) {
                        totalSize = readSize;
                    }
                    break;
                }
                totalSize += readSize;
            }
            if (totalSize != 0 || !hasRemain) {
                CompletionHandler<Number, Object> completionHandler = readCompletionHandler;
                Object attach = readAttachment;
                Scattering scattering = readScattering;
                resetRead();
                if (scattering == null) {
                    int size = (int) totalSize;
                    completionHandler.completed(size, attach);
                } else {
                    completionHandler.completed(totalSize, attach);
                }

                if (!readPending && readSelectionKey != null) {
                    group.removeOps(readSelectionKey, SelectionKey.OP_READ);
                }
            } else if (readSelectionKey == null) {
                readWorker.addRegister(new WorkerRegister() {
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
                group.interestOps(readWorker, readSelectionKey, SelectionKey.OP_READ);
            }

        } catch (IOException e) {
            readCompletionHandler.failed(e, readAttachment);
        }
    }

    private void resetRead() {
        readPending = false;
        readFuture = null;
        readCompletionHandler = null;
        readAttachment = null;
        readBuffer = null;
        readScattering = null;
    }

    public void doWrite() {
        try {
            //此前通过Future调用,且触发了cancel
            if (writeFuture != null && writeFuture.isDone()) {
                resetWrite();
                return;
            }
            //非writeWorker线程允许无限递归输出,事实上也不会出现该场景
            //writeWorker递归回调限制上线EnhanceAsynchronousChannelGroup.MAX_INVOKER
            boolean directWrite = Thread.currentThread() == writeWorker.getWorkerThread()
                    && writeWorker.getInvoker().getAndIncrement() < EnhanceAsynchronousChannelGroup.MAX_INVOKER;
            long totalSize = 0;
            long writeSize;
            boolean hasRemain = true;
            while (directWrite && hasRemain) {
                if (writeScattering != null) {
                    writeSize = channel.write(writeScattering.getBuffers(), writeScattering.getOffset(), writeScattering.getLength());
                    hasRemain = hasRemaining(writeScattering);
                } else {
                    writeSize = channel.write(writeBuffer);
                    hasRemain = writeBuffer.hasRemaining();
                }
                if (writeSize <= 0) {
                    if (totalSize == 0) {
                        totalSize = writeSize;
                    }
                    break;
                }
                totalSize += writeSize;
            }

            if (totalSize > 0 || !hasRemain) {
                CompletionHandler<Number, Object> completionHandler = writeCompletionHandler;
                Object attach = writeAttachment;
                Scattering scattering = writeScattering;
                resetWrite();
                if (scattering == null) {
                    int size = (int) totalSize;
                    completionHandler.completed(size, attach);
                } else {
                    completionHandler.completed(totalSize, attach);
                }

            } else if (writeSelectionKey == null) {
                writeWorker.addRegister(new WorkerRegister() {
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
                group.interestOps(writeWorker, writeSelectionKey, SelectionKey.OP_WRITE);
            }
        } catch (IOException e) {
            writeCompletionHandler.failed(e, writeAttachment);
        }
    }

    private boolean hasRemaining(Scattering scattering) {
        for (int i = 0; i < scattering.getLength(); i++) {
            if (scattering.getBuffers()[scattering.getOffset() + i].hasRemaining()) {
                return true;
            }
        }
        return false;
    }

    private void resetWrite() {
        writePending = false;
        writeFuture = null;
        writeAttachment = null;
        writeCompletionHandler = null;
        writeBuffer = null;
        writeScattering = null;
    }

    @Override
    public boolean isOpen() {
        return channel.isOpen();
    }
}
