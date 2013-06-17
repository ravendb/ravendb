package raven.abstractions.closure;

public class Actions {

  public static class Delegate3<X, Y, Z> implements Action3<X, Y, Z> {
    @Override
    public void apply(X first, Y second, Z third) {
      // do nothing
    }
  }

  public static <X, Y, Z> Action3<X, Y, Z> delegate3() {
    return new Delegate3<>();
  }
}
