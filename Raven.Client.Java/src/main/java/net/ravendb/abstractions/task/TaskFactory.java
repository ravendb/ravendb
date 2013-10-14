package net.ravendb.abstractions.task;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import net.ravendb.abstractions.closure.Function0;


import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
//TODO: do we really need this executor service?
public class TaskFactory {

  private final static ListeningExecutorService executorService;

  static {
    int coreThreadCount = Runtime.getRuntime().availableProcessors() * 2;
    ExecutorService fixedThreadPool = Executors.newFixedThreadPool(coreThreadCount, new ThreadFactory() {

      @Override
      public Thread newThread(Runnable r) {
        Thread thread = Executors.defaultThreadFactory().newThread(r);
        thread.setDaemon(true);
        return thread;
      }
    });
    executorService = MoreExecutors.listeningDecorator(fixedThreadPool);
  }


  public static <T> void startNew(final Function0<T> function0, FutureCallback<T> continueWith) {
    ListenableFutureTask<T> listenableFutureTask = ListenableFutureTask.create(new Callable<T>() {
      @Override
      public T call() throws Exception {
        return function0.apply();
      }
    });

    Futures.addCallback(listenableFutureTask, continueWith, executorService);
    executorService.submit(listenableFutureTask);
  }


  public static <T> ListenableFuture<T> startNew(final Function0<T> function0) {

    return executorService.submit(new Callable<T>() {
      @Override
      public T call() throws Exception {
        return function0.apply();
      }
    });
  }



}
