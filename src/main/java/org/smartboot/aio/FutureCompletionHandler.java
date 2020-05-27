package org.smartboot.aio;

import java.nio.channels.CompletionHandler;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

class FutureCompletionHandler<V, A> implements CompletionHandler<V, A>, Future<V>, Runnable {
    private CompletionHandler<V, A> completionHandler;
    private A attach;
    private V result;
    private boolean done = false;
    private boolean cancel = false;
    private Throwable exception;

    public FutureCompletionHandler(CompletionHandler<V, A> completionHandler, A attach) {
        this.completionHandler = completionHandler;
        this.attach = attach;
    }

    public FutureCompletionHandler() {
    }

    @Override
    public void completed(V result, A selectionKey) {
        this.result = result;
        done = true;
        synchronized (this) {
            this.notify();
        }
        if (completionHandler != null) {
            completionHandler.completed(result, attach);
        }
    }

    @Override
    public void failed(Throwable exc, A attachment) {
        exception = exc;
        done = true;
        if (completionHandler != null) {
            completionHandler.failed(exc, attachment);
        }
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        if (done || cancel) {
            return false;
        }
        cancel = true;
        done = true;
        synchronized (this) {
            notify();
        }
        return true;
    }

    @Override
    public boolean isCancelled() {
        return cancel;
    }

    @Override
    public boolean isDone() {
        return done || cancel;
    }

    @Override
    public synchronized V get() throws InterruptedException, ExecutionException {
        if (done) {
            if (exception != null) {
                throw new ExecutionException(exception);
            }
            return result;
        } else {
            wait();
        }
        return get();
    }

    @Override
    public synchronized V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        if (done) {
            return get();
        } else {
            wait(unit.toMillis(timeout));
        }
        if (done) {
            return get();
        }
        throw new TimeoutException();
    }

    @Override
    public synchronized void run() {
        if (!done) {
            cancel(true);
            completionHandler.failed(new TimeoutException(), attach);
        }
    }
}
