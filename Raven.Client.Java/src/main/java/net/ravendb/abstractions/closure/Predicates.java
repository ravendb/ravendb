package net.ravendb.abstractions.closure;


public class Predicates {
  public static class True<T> implements Predicate<T> {
    @Override
    public Boolean apply(T input) {
      return true;
    }
  }

  public static class False<T> implements Predicate<T> {
    @Override
    public Boolean apply(T input) {
      return false;
    }
  }

  public static class AndPredicate<T> implements Predicate<T> {
    private Predicate<T> left;
    private Predicate<T> right;

    private AndPredicate(Predicate<T> left, Predicate<T> right) {
      super();
      this.left = left;
      this.right = right;
    }

    @Override
    public Boolean apply(T input) {
      return left.apply(input) && right.apply(input);
    }
  }

  public static <T> Predicate<T> alwaysTrue() {
    return new True<>();
  }

  public static <T> Predicate<T> alwaysFalse() {
    return new False<>();
  }

  public static <T> Predicate<T> and(Predicate<T> left, Predicate<T> right) {
    return new AndPredicate<>(left, right);
  }

}
