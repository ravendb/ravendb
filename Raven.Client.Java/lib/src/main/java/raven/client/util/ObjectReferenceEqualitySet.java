package raven.client.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Set that has custom comparer: == instread of .equals()
 * @param <T>
 */
public class ObjectReferenceEqualitySet<T> implements Set<T> {
  private List<T> data = new ArrayList<>();

  @Override
  public int size() {
    return data.size();
  }

  @Override
  public boolean isEmpty() {
    return data.isEmpty();
  }

  @Override
  public boolean contains(Object o) {
    for (T item: data) {
      if (item == o) {
        return true;
      }
    }
    return false;
  }

  @Override
  public Iterator<T> iterator() {
    return data.iterator();
  }

  @Override
  public Object[] toArray() {
    return data.toArray();
  }

  @SuppressWarnings({ "unchecked", "hiding" })
  @Override
  public <T> T[] toArray(T[] a) {
    return (T[]) data.toArray();
  }

  @Override
  public boolean add(T e) {
    if (!contains(e)) {
      data.add(e);
      return true;
    }
    return false;
  }

  @Override
  public boolean remove(Object o) {
    int index = -1;
    for (int i = 0; i < data.size(); i++) {
      if (data.get(i) == o) {
        index = i;
        break;
      }
    }
    if (index != -1) {
      data.remove(index);
      return true;
    }
    return false;
  }

  @Override
  public boolean containsAll(Collection< ? > c) {
    for (Object item: c) {
      if (!contains(item)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean addAll(Collection< ? extends T> c) {
    boolean changed = false;
    for (T newItem: c) {
      changed |= add(newItem);
    }
    return changed;
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean retainAll(Collection< ? > c) {
    boolean changed = false;
    List<T> newList = new ArrayList<>(data.size());
    for (Object o: c) {
      if (contains(o)) {
        newList.add((T) o);
      }
    }
    changed = data.size() != newList.size();
    data = newList;
    return changed;
  }

  @Override
  public boolean removeAll(Collection< ? > c) {
    boolean changed = false;
    for (Object item: c) {
      changed |= remove(item);
    }
    return changed;
  }

  @Override
  public void clear() {
    data.clear();
  }

}
