package net.ravendb.abstractions.data;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.ravendb.abstractions.basic.SharpEnum;

import org.apache.commons.lang.StringUtils;



public class EnumSet<T extends Enum<T>, S extends EnumSet<T, S>> {
  protected Class<T> innerClass;
  protected Class<S> innerSetClass;
  protected Method getValueMethod;
  protected long storage = 0;

  public EnumSet(Class<T> innerClass, List<? extends Enum<T>> values) {
    this(innerClass);

    for (Enum<?> e: values) {
      storage |= getValue(e);
    }

  }

  public void setValue(long value) {
    this.storage = value;
  }

  public long getValue() {
    return storage;
  }

  private int getValue(Enum<?> prop) {
    try {
      return (int) getValueMethod.invoke(prop);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }


  public EnumSet(Class<T> innerClass) {
    this.innerClass = innerClass;
    try {
      getValueMethod = innerClass.getMethod("getValue");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public Class<T> getInnerClass() {
    return innerClass;
  }

  public boolean contains(T value) {
    int intVal = getValue(value);
    if (intVal == 0) {
      return storage == 0;
    }
    return (intVal | storage) == storage;
  }

  public void add(T value) {
    int intVal = getValue(value);
    storage |= intVal;
  }

  public void remove(T value) {
    int intVal = getValue(value);
    storage &= ~intVal;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((innerClass == null) ? 0 : innerClass.getName().hashCode());
    result = prime * result + (int) (storage ^ (storage >>> 32));
    return result;
  }

  @SuppressWarnings("rawtypes")
  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    EnumSet other = (EnumSet) obj;
    if (innerClass == null) {
      if (other.innerClass != null) return false;
    } else if (!innerClass.getName().equals(other.innerClass.getName())) return false;
    if (storage != other.storage) return false;
    return true;
  }

  @Override
  public S clone() {
    try {
      S newInstance = innerSetClass.newInstance();
      newInstance.storage = this.storage;
      return newInstance;
    } catch (InstantiationException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  protected static <S extends EnumSet<?, ?>> S construct(S newSet, int value) {
    newSet.storage = value;
    return newSet;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  protected static <S extends EnumSet> S construct(S newSet, String value) {

    Map<String, Enum<?>> lookup = new HashMap<>();
    Object[] enumConsts = newSet.innerClass.getEnumConstants();
    for (Object enumConst : enumConsts) {
      lookup.put(SharpEnum.value((Enum<?>)enumConst), (Enum<?>) enumConst);
    }
    String[] tokens = value.split(",");
    for (String token: tokens) {
      token = token.trim();
      if (!StringUtils.isEmpty(token)) {
        if (!lookup.containsKey(token)) {
          throw new IllegalStateException("Unable to find enum constant for:" + token);
        }
        newSet.add(lookup.get(token));
      }
    }
    return newSet;
  }


}
