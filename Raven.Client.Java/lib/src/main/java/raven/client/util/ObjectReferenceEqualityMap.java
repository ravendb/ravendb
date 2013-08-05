package raven.client.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.NotImplementedException;

/**
 * Map that uses == instead of equals to compare elements
 *
 * @param <K>
 * @param <V>
 */
public class ObjectReferenceEqualityMap<K, V> implements Map<K, V> {

  private List<K> keys = new ArrayList<>();
  private List<V> values = new ArrayList<>();

  @Override
  public int size() {
    return keys.size();
  }

  @Override
  public boolean isEmpty() {
    return keys.isEmpty();
  }

  @Override
  public boolean containsKey(Object key) {
    for (K k: keys) {
      if (k == key) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean containsValue(Object value) {
    for (V v: values) {
      if (v == value) {
        return true;
      }
    }
    return false;
  }

  @Override
  public V get(Object key) {
    for (int i = 0 ; i < keys.size(); i++) {
      if (keys.get(i) == key) {
        return values.get(i);
      }
    }
    return null;
  }

  @Override
  public V put(K key, V value) {
    V prevValue = null;
    for (int i = 0; i < keys.size(); i++) {
      if (keys.get(i) == key) {
        prevValue = values.get(i);
        values.set(i, value);
        return prevValue;
      }
    }
    keys.add(key);
    values.add(value);
    return null;
  }

  @Override
  public V remove(Object key) {
    for (int i = 0; i < keys.size(); i++) {
      if (keys.get(i) == key) {
        V prevValue = values.get(i);
        keys.remove(i);
        values.remove(i);
        return prevValue;
      }
    }
    return null;
  }

  @Override
  public void putAll(Map< ? extends K, ? extends V> m) {
    for (Map.Entry<? extends K, ? extends V> entry: m.entrySet()) {
      put(entry.getKey(), entry.getValue());
    }
  }

  @Override
  public void clear() {
    keys.clear();
    values.clear();
  }

  @Override
  public Set<K> keySet() {
    ObjectReferenceEqualitySet<K> result = new ObjectReferenceEqualitySet<>();
    for (K key: keys) {
      result.add(key);
    }
    return result;
  }

  @Override
  public Collection<V> values() {
    return values;
  }

  @Override
  public Set<Map.Entry<K, V>> entrySet() {
    ObjectReferenceEqualitySet<Map.Entry<K, V>> result = new ObjectReferenceEqualitySet<>();
    for (int i = 0 ; i < keys.size(); i++) {
      result.add(new Entry<K, V>(keys.get(i), values.get(i)));
    }
    return result;
  }

  static class Entry<K, V> implements Map.Entry<K, V> {

    private K key;
    private V value;


    Entry(K key, V value) {
      super();
      this.key = key;
      this.value = value;
    }

    @Override
    public K getKey() {
      return key;
    }

    @Override
    public V getValue() {
      return value;
    }

    @Override
    public V setValue(V value) {
      throw new NotImplementedException();
    }

  }



}
