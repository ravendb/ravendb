package raven.client.changes;


public interface IObserver<T> {
  public void onNext(T value);

  public void onError(Exception error);

  public void onCompleted();
}
