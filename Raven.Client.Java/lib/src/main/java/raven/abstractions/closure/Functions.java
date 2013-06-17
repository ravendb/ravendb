package raven.abstractions.closure;

public class Functions {
  public static  class AlwaysTrue<S> implements Function1<S, Boolean> {
    @Override
    public Boolean apply(S input) {
      return true;
    }
  }

  public static  class AlwaysFalse<S> implements Function1<S, Boolean> {
    @Override
    public Boolean apply(S input) {
      return false;
    }
  }

  public static <T> Function1<T, Boolean> alwaysTrue() {
    return new AlwaysTrue<T>();
  }

  public static <T> Function1<T, Boolean> alwaysFalse() {
    return new AlwaysFalse<T>();
  }
}
