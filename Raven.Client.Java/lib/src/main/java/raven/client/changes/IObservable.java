package raven.client.changes;

import java.io.Closeable;


public interface IObservable<T> {
  public Closeable subscribe(IObserver<T> observer);
}
