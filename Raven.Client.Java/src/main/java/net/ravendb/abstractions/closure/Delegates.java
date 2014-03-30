package net.ravendb.abstractions.closure;

public class Delegates {

  public static class Delegate0 implements Action0 {
    @Override
    public void apply() {
      // do nothing
    }
  }

  public static class Delegate1<X> implements Action1<X> {
    @Override
    public void apply(X first) {
      // do nothing
    }
  }

  public static class Delegate2<X, Y> implements Action2<X, Y> {
    @Override
    public void apply(X first, Y second) {
      // do nothing
    }
  }

  public static class Delegate3<X, Y, Z> implements Action3<X, Y, Z> {
    @Override
    public void apply(X first, Y second, Z third) {
      // do nothing
    }
  }

  public static Action0 delegate0() {
    return new Delegate0();
  }


  public static <X> Action1<X> delegate1() {
    return new Delegate1<>();
  }

  public static <X, Y> Action2<X, Y> delegate2() {
    return new Delegate2<>();
  }

  public static <X, Y, Z> Action3<X, Y, Z> delegate3() {
    return new Delegate3<>();
  }

  public static <X> Action1<X> combine(final Action1<X> first, final Action1<X> second) {
    if (first == null) {
      return second;
    }
    if (second == null) {
      return first;
    }
    return new Action1<X>() {

      @Override
      public void apply(X input) {
          first.apply(input);
          second.apply(input);
      }
    };
  }


}
