package raven.client.changes;

import java.io.Closeable;

import raven.abstractions.closure.Predicate;


public interface IObservable<T> {
  public Closeable subscribe(IObserver<T> observer);


  public IObservable<T> where(Predicate<T> predicate);

}
