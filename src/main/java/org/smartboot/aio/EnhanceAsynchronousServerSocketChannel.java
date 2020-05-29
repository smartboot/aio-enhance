package org.smartboot.aio;

import java.io.IOException;
import java.net.SocketAddress;
import java.net.SocketOption;
import java.nio.channels.AcceptPendingException;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.CompletionHandler;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author 三刀
 * @version V1.0 , 2020/5/25
 */
class EnhanceAsynchronousServerSocketChannel extends AsynchronousServerSocketChannel {
    private final ServerSocketChannel serverSocketChannel;
    private final EnhanceAsynchronousChannelGroup enhanceAsynchronousChannelGroup;
    private final AtomicInteger invoker = new AtomicInteger(0);
    private CompletionHandler<AsynchronousSocketChannel, Object> acceptCompletionHandler;
    private FutureCompletionHandler<AsynchronousSocketChannel, Void> acceptFuture;
    private Object attachment;
    private SelectionKey selectionKey;
    private SocketChannel waitFinishChannel;
    private boolean acceptPending;

    /**
     * Initializes a new instance of this class.
     */
    protected EnhanceAsynchronousServerSocketChannel(EnhanceAsynchronousChannelGroup enhanceAsynchronousChannelGroup) throws IOException {
        super(enhanceAsynchronousChannelGroup.provider());
        this.enhanceAsynchronousChannelGroup = enhanceAsynchronousChannelGroup;
        serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.configureBlocking(false);
        System.out.println("enhance...");
    }

    @Override
    public AsynchronousServerSocketChannel bind(SocketAddress local, int backlog) throws IOException {
        serverSocketChannel.bind(local, backlog);
        return this;
    }

    @Override
    public <T> AsynchronousServerSocketChannel setOption(SocketOption<T> name, T value) throws IOException {
        serverSocketChannel.setOption(name, value);
        return this;
    }

    @Override
    public <T> T getOption(SocketOption<T> name) throws IOException {
        return serverSocketChannel.getOption(name);
    }

    @Override
    public Set<SocketOption<?>> supportedOptions() {
        return serverSocketChannel.supportedOptions();
    }

    @Override
    public <A> void accept(A attachment, CompletionHandler<AsynchronousSocketChannel, ? super A> handler) {
        if (acceptPending) {
            throw new AcceptPendingException();
        }
        acceptPending = true;
        if (waitFinishChannel != null) {
            try {
                waitFinishChannel.finishConnect();
            } catch (IOException e) {
                acceptCompletionHandler.failed(e, attachment);
            } finally {
                waitFinishChannel = null;
            }
        }
        this.acceptCompletionHandler = (CompletionHandler<AsynchronousSocketChannel, Object>) handler;
        this.attachment = attachment;
        doAccept();
    }

    public void doAccept() {
        try {
            //此前通过Future调用,且触发了cancel
            if (acceptFuture != null && acceptFuture.isDone()) {
                acceptPending = false;
                acceptFuture = null;
                if (selectionKey != null) {
                    enhanceAsynchronousChannelGroup.removeOps(selectionKey, SelectionKey.OP_ACCEPT);
                }
                return;
            }
            if (invoker.getAndIncrement() > EnhanceAsynchronousChannelGroup.MAX_INVOKER) {
                invoker.set(0);
                enhanceAsynchronousChannelGroup.interestOps(selectionKey, SelectionKey.OP_ACCEPT);
                return;
            }
            SocketChannel socketChannel = serverSocketChannel.accept();
            if (socketChannel != null) {
                EnhanceAsynchronousSocketChannel asynchronousSocketChannel = new EnhanceAsynchronousSocketChannel(enhanceAsynchronousChannelGroup, socketChannel);
                acceptPending = false;
                waitFinishChannel = socketChannel;
                asynchronousSocketChannel.getReadInvoker().set(EnhanceAsynchronousChannelGroup.MAX_INVOKER);
                acceptCompletionHandler.completed(asynchronousSocketChannel, attachment);
                if (waitFinishChannel != null) {
                    socketChannel.finishConnect();
                    waitFinishChannel = null;
                }
                if (!acceptPending && selectionKey != null) {
                    enhanceAsynchronousChannelGroup.removeOps(selectionKey, SelectionKey.OP_ACCEPT);
                }
            }
            //首次注册selector
            else {
                invoker.set(0);
                if (selectionKey == null) {
                    enhanceAsynchronousChannelGroup.getAcceptWorker().addRegister(new WorkerRegister() {
                        @Override
                        public void callback(Selector selector) {
                            try {
                                selectionKey = serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
                                selectionKey.attach(EnhanceAsynchronousServerSocketChannel.this);
                            } catch (ClosedChannelException e) {
                                acceptCompletionHandler.failed(e, attachment);
                            }
                        }
                    });
                } else {
                    enhanceAsynchronousChannelGroup.interestOps(selectionKey, SelectionKey.OP_ACCEPT);
                }
            }
        } catch (IOException e) {
            this.acceptCompletionHandler.failed(e, attachment);
        }

    }

    @Override
    public Future<AsynchronousSocketChannel> accept() {
        FutureCompletionHandler<AsynchronousSocketChannel, Void> acceptFuture = new FutureCompletionHandler<>();
        accept(null, acceptFuture);
        this.acceptFuture = acceptFuture;
        return acceptFuture;
    }

    @Override
    public SocketAddress getLocalAddress() throws IOException {
        return serverSocketChannel.getLocalAddress();
    }

    @Override
    public boolean isOpen() {
        return serverSocketChannel.isOpen();
    }

    @Override
    public void close() throws IOException {
        serverSocketChannel.close();
    }
}
