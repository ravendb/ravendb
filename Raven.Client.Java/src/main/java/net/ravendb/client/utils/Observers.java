package net.ravendb.client.utils;

import net.ravendb.abstractions.closure.Action1;
import net.ravendb.client.changes.IObserver;


public class Observers {
  public static <T> IObserver<T> create(Action1<T> action) {
    return new ActionBasedObserver<>(action);
  }

  public static class ActionBasedObserver<T> implements IObserver<T> {
    private Action1<T> action;

    public ActionBasedObserver(Action1<T> action) {
      super();
      this.action = action;
    }
    @Override
    public void onNext(T value) {
      action.apply(value);
    }

    @Override
    public void onError(Exception error) {
      //empty
    }

    @Override
    public void onCompleted() {
      //empty

    }
  }

}
