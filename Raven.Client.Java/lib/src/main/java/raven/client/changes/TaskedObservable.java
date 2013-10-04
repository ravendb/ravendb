package raven.client.changes;

import java.io.Closeable;
import java.io.IOException;

import raven.abstractions.closure.Function1;
import raven.client.connection.profiling.ConcurrentSet;


public class TaskedObservable<T> implements IObservable<T> {
  private final LocalConnectionState localConnectionState;
  private final Function1<T, Boolean> filter;
  private final ConcurrentSet<IObserver<T>> subscribers = new ConcurrentSet<>();

  public TaskedObservable(LocalConnectionState localConnectionState, Function1<T, Boolean> filter) {
    this.localConnectionState = localConnectionState;
    this.filter = filter;
  }

  @Override
  public Closeable subscribe(final IObserver<T> observer) {
    localConnectionState.inc();
    subscribers.add(observer);
    return new Closeable() {

      @Override
      public void close() throws IOException {
        localConnectionState.dec();
        subscribers.remove(observer);
      }
    };
  }

  public void send(T msg) {
    try {
      if (!filter.apply(msg)) {
        return;
      }
    } catch (Exception e) {
      error(e);
      return;
    }

    for (IObserver<T> subscriber : subscribers) {
      subscriber.onNext(msg);
    }
  }

  public void error(Exception obj) {
    for (IObserver<T> subscriber : subscribers) {
      subscriber.onError(obj);
    }
  }

}
