package net.ravendb.client.changes;

import java.io.Closeable;

import net.ravendb.abstractions.closure.Predicate;



public interface IObservable<T> {
  public Closeable subscribe(IObserver<T> observer);


  public IObservable<T> where(Predicate<T> predicate);

}
