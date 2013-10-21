package net.ravendb.abstractions.closure;

public class Functions {
  public static  class AlwaysTrue<S> implements Function1<S, Boolean> {
    @Override
    public Boolean apply(S input) {
      return true;
    }
  }

  public static class AlwaysFalse<S> implements Function1<S, Boolean> {
    @Override
    public Boolean apply(S input) {
      return false;
    }
  }

  /**
   * Function that takes 1 parameter and returns always the same value
   *
   */
  public static class StaticFunction1<S, T> implements Function1<S, T> {

    private T inner;
    public StaticFunction1(T value) {
      this.inner = value;
    }

    @Override
    public T apply(S input) {
      return inner;
    }

  }


  public static <T> Function1<T, Boolean> alwaysTrue() {
    return new AlwaysTrue<>();
  }

  public static <T> Function1<T, Boolean> alwaysFalse() {
    return new AlwaysFalse<>();
  }

}
