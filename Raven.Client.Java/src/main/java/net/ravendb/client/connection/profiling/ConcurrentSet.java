package net.ravendb.client.connection.profiling;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


public class ConcurrentSet<T> implements Set<T> {

  private ConcurrentMap<T, Object> innerMap;

  private final static Object value = new Object();

  public ConcurrentSet() {
    innerMap = new ConcurrentHashMap<>();
  }

  @Override
  public int size() {
    return innerMap.size();
  }

  @Override
  public boolean isEmpty() {
    return innerMap.isEmpty();
  }

  @Override
  public boolean contains(Object o) {
    return innerMap.containsKey(o);
  }

  @Override
  public Iterator<T> iterator() {
    return innerMap.keySet().iterator();
  }

  @Override
  public Object[] toArray() {
    return innerMap.keySet().toArray();
  }

  @Override
  public <S> S[] toArray(S[] a) {
    return innerMap.keySet().toArray(a);
  }

  @Override
  public boolean add(T e) {
    return innerMap.put(e, value) == null;
  }

  @Override
  public boolean remove(Object o) {
    return innerMap.remove(o) ==null;
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    return innerMap.keySet().containsAll(c);
  }

  @Override
  public boolean addAll(Collection<? extends T> c) {
    boolean changed = false;
    for (T item: c) {
      changed &= innerMap.put(item, value) == null;
    }
    return changed;
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    return innerMap.keySet().retainAll(c);
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    return innerMap.keySet().removeAll(c);
  }

  @Override
  public void clear() {
    innerMap.clear();
  }
}
