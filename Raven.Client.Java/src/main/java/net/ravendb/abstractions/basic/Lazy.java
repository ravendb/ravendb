package net.ravendb.abstractions.basic;

import net.ravendb.abstractions.closure.Function0;

public class Lazy<T> {
  private Function0<T> valueFactory;
  private boolean valueCreated = false;
  private T value;


  public Lazy(Function0<T> valueFactory) {
    this.valueFactory = valueFactory;
  }

  public boolean isValueCreated() {
    return valueCreated;
  }

  public T getValue() {
    if (valueCreated) {
      return value;
    } else {
      synchronized (this) {
       if (!valueCreated) {
         value = valueFactory.apply();
         valueCreated = true;
       }
      }
      return value;
    }
  }

}
