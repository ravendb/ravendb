package net.ravendb.client.util;

import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Set;

public class IdentityHashSet<T> implements Set<T> {

  private IdentityHashMap<T, Void> inner = new IdentityHashMap<>();

  @Override
  public int size() {
    return inner.size();
  }

  @Override
  public boolean isEmpty() {
    return inner.isEmpty();
  }

  @Override
  public boolean contains(Object o) {
    return inner.containsKey(o);
  }

  @Override
  public Iterator<T> iterator() {
    return inner.keySet().iterator();
  }

  @Override
  public Object[] toArray() {
    return inner.keySet().toArray();
  }

  @Override
  public <S> S[] toArray(S[] a) {
    return inner.keySet().toArray(a);
  }

  @Override
  public boolean add(T e) {
    boolean result = inner.containsKey(e);
    inner.put(e, null);
    return result;
  }

  @Override
  public boolean remove(Object o) {
    boolean result = inner.containsKey(o);
    inner.remove(o);
    return result;
  }

  @Override
  public boolean containsAll(Collection< ? > c) {
    for (Object o: c) {
      if (!inner.containsKey(o)) {
        return false;
      }
    }

    return true;
  }

  @Override
  public boolean addAll(Collection< ? extends T> c) {
    boolean result = false;
    for (T o: c) {
     result |= add(o);
    }

    return result;
  }

  @Override
  public boolean retainAll(Collection< ? > c) {
    IdentityHashMap<T, Void> copy = new IdentityHashMap<>();
    for (T item: inner.keySet()) {
      if (c.contains(item)) {
        copy.put(item, null);
      }
    }
    inner = copy;
    return !c.isEmpty();
  }

  @Override
  public boolean removeAll(Collection< ? > c) {
    boolean result = false;
    for(Object o: c) {
      result |= remove(o);
    }
    return result;
  }

  @Override
  public void clear() {
    inner.clear();
  }


}
