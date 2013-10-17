package net.ravendb.abstractions.json.linq;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import net.ravendb.abstractions.basic.Reference;


public class DictionaryWithParentSnapshot implements Map<String, RavenJToken>, Iterable<Map.Entry<String, RavenJToken>> {

  private final Comparator<String> comparer;

  private static final RavenJToken DELETED_MARKER = new RavenJValue("*DeletedMarker*", JTokenType.NULL);

  private final DictionaryWithParentSnapshot parentSnapshot;
  private int count;
  private Map<String, RavenJToken> localChanges;
  private String snapshotMsg;
  private boolean snapshot;

  public Map<String, RavenJToken> getLocalChanges() {
    if (localChanges == null) {
      localChanges = new TreeMap<> (comparer);
    }
    return localChanges;
  }

  public DictionaryWithParentSnapshot(Comparator<String> comparer) {
    this.comparer = comparer;
    this.parentSnapshot = null;
  }


  public DictionaryWithParentSnapshot(DictionaryWithParentSnapshot previous) {
    this.parentSnapshot = previous;
    this.comparer = previous.comparer;
  }

  /* (non-Javadoc)
   * @see java.util.HashMap#put(java.lang.Object, java.lang.Object)
   */
  @Override
  public RavenJToken put(String key, RavenJToken value) {
    if (isSnapshot()) {
      throw new IllegalStateException(snapshotMsg != null ? snapshotMsg
        : "Cannot modify a snapshot, this is probably a bug");
    }
    if (!containsKey(key)) {
      count++;
    }
    getLocalChanges().put(key, value);
    return value;
  }

  /* (non-Javadoc)
   * @see java.util.Map#containsKey(java.lang.Object)
   */
  @Override
  public boolean containsKey(Object keyObject) {
    String key = (String) keyObject;
    RavenJToken token;
    if (getLocalChanges().containsKey(key)) {
      token = getLocalChanges().get(key);
      if (token == DELETED_MARKER) {
        return false;
      }
      return true;
    }
    return (parentSnapshot != null && parentSnapshot.containsKey(key) && parentSnapshot.get(key) != DELETED_MARKER);
  }

  /* (non-Javadoc)
   * @see java.util.Map#keySet()
   */
  @Override
  public Set<String> keySet() {
    if (getLocalChanges().isEmpty()) {
      if (parentSnapshot != null) {
        return parentSnapshot.keySet();
      }
      return new HashSet<>();
    }

    Set<String> ret = new HashSet<>();
    if (parentSnapshot != null) {
      for (String key : parentSnapshot.keySet()) {
        if (!getLocalChanges().containsKey(key)) {
          ret.add(key);
        }
      }
    }
    for (String key : getLocalChanges().keySet()) {
      if (getLocalChanges().containsKey(key) && getLocalChanges().get(key) != DELETED_MARKER) {
        ret.add(key);
      }
    }

    return ret;
  }

  /* (non-Javadoc)
   * @see java.util.Map#remove(java.lang.Object)
   */
  @Override
  public RavenJToken remove(Object keyObject) {
    String key = (String) keyObject;
    if (isSnapshot()) {
      throw new IllegalStateException("Cannot modify a snapshot, this is probably a bug");
    }
    boolean parentHasIt = false;
    RavenJToken parentToken = null;

    if (parentSnapshot != null) {
      parentToken = parentSnapshot.get(key);
      if (parentToken != null) {
        parentHasIt = true;
      }
    }

    if (!getLocalChanges().containsKey(key)) {
      if (parentHasIt && parentToken != DELETED_MARKER) {
        getLocalChanges().put(key, DELETED_MARKER);
        count--;
        return parentToken;
      }
      return null;
    }
    RavenJToken token = getLocalChanges().get(key);
    if (token == DELETED_MARKER) {
      return null;
    }
    count--;
    getLocalChanges().put(key, DELETED_MARKER);
    return token;
  }

  /* (non-Javadoc)
   * @see java.util.Map#get(java.lang.Object)
   */
  @Override
  public RavenJToken get(Object keyObject) {
    String key = (String) keyObject;
    if (getLocalChanges().containsKey(key)) {
      RavenJToken unsafeVal = getLocalChanges().get(key);
      if (unsafeVal == DELETED_MARKER) {
        return null;
      }
      return unsafeVal;
    }
    if (parentSnapshot == null || !parentSnapshot.containsKey(key) || parentSnapshot.get(key) == DELETED_MARKER) {
      return null;
    }
    RavenJToken unsafeVal = parentSnapshot.get(key);
    if (!isSnapshot() && unsafeVal != null) {
      if (!unsafeVal.isSnapshot() && unsafeVal.getType() != JTokenType.OBJECT) {
        unsafeVal.ensureCannotBeChangeAndEnableShapshotting();
      }
    }
    return unsafeVal;
  }

  /* (non-Javadoc)
   * @see java.util.Map#values()
   */
  @Override
  public Collection<RavenJToken> values() {
    Collection<RavenJToken> tokens = new ArrayList<>();
    for (String key : keySet()) {
      tokens.add(get(key));
    }
    return tokens;
  }

  public boolean isSnapshot() {
    return snapshot;
  }

  public DictionaryWithParentSnapshot createSnapshot() {
    if (!isSnapshot()) {
      throw new IllegalStateException("Cannot create snapshot without previously calling EnsureSnapshot");
    }
    return new DictionaryWithParentSnapshot(this);
  }

  /* (non-Javadoc)
   * @see java.util.Map#clear()
   */
  @Override
  public void clear() {
    for (String key : keySet()) {
      remove(key);
    }
  }

  /* (non-Javadoc)
   * @see java.util.Map#isEmpty()
   */
  @Override
  public boolean isEmpty() {
    return size() == 0;
  }

  /* (non-Javadoc)
   * @see java.util.Map#putAll(java.util.Map)
   */
  @Override
  public void putAll(Map<? extends String, ? extends RavenJToken> m) {
    for (String key : m.keySet()) {
      put(key, m.get(key));
    }
  }

  /* (non-Javadoc)
   * @see java.util.Map#containsValue(java.lang.Object)
   */
  @Override
  public boolean containsValue(Object value) {
    throw new IllegalStateException("Not implemeneted");
  }

  /* (non-Javadoc)
   * @see java.util.Map#entrySet()
   */
  @Override
  public Set<Entry<String, RavenJToken>> entrySet() {
    Set<Entry<String, RavenJToken>> entries = new HashSet<>();
    Iterator<java.util.Map.Entry<String, RavenJToken>> iterator = iterator();
    while (iterator.hasNext()) {
      entries.add(iterator.next());
    }
    return entries;
  }

  /* (non-Javadoc)
   * @see java.util.Map#size()
   */
  @Override
  public int size() {
    if (parentSnapshot != null) {
      return count + parentSnapshot.size();
    }
    return count;
  }

  public void ensureSnapshot() {
    ensureSnapshot(null);
  }

  public void ensureSnapshot(String msg) {
    snapshot = true;
    snapshotMsg = msg;
  }

  @Override
  public Iterator<java.util.Map.Entry<String, RavenJToken>> iterator() {
    return new DictionaryInterator();
  }

  public boolean tryGetValue(String key, Reference<RavenJToken> value) {
    value.value = null;
    Reference<RavenJToken> unsafeVal = new Reference<>();
    if (getLocalChanges() != null && getLocalChanges().containsKey(key)) {
      unsafeVal.value = getLocalChanges().get(key);
      if (DELETED_MARKER.equals(unsafeVal.value)) return false;

      value.value = unsafeVal.value;
      return true;
    }

    if (parentSnapshot == null ||
      !parentSnapshot.tryGetValue(key, unsafeVal) ||
      DELETED_MARKER.equals(unsafeVal.value)) return false;

    if (!isSnapshot() && unsafeVal.value != null) {
      if (unsafeVal.value.isSnapshot() && JTokenType.OBJECT.equals(unsafeVal.value.getType()))
        unsafeVal.value.ensureCannotBeChangeAndEnableShapshotting();
    }

    value.value = unsafeVal.value;

    return true;
  }

  class DictionaryInterator implements Iterator<java.util.Map.Entry<String, RavenJToken>> {

    private boolean parentProcessed = false;
    private Iterator<java.util.Map.Entry<String, RavenJToken>> parentIterator;
    private Iterator<java.util.Map.Entry<String, RavenJToken>> localIterator;

    private Iterator<java.util.Map.Entry<String, RavenJToken>> getCurrentIterator() {
      if (!parentProcessed) {
        if (parentSnapshot != null) {
          if (parentIterator == null) {
            parentIterator = parentSnapshot.iterator();
          }
          if (parentIterator.hasNext()) {
            return parentIterator;
          } else {
            //no more elements in parent iterator
            parentProcessed = true;
          }
        } else {
          parentProcessed = true;
        }
      }
      if (localIterator == null) {
        Map<String, RavenJToken> entrySetMap = new HashMap<>();
        for (Map.Entry<String, RavenJToken> entry: getLocalChanges().entrySet()) {
          if (entry.getValue() != DELETED_MARKER) {
            entrySetMap.put(entry.getKey(), entry.getValue());
          }
        }
        localIterator = entrySetMap.entrySet().iterator();

      }
      return localIterator;
    }

    @Override
    public boolean hasNext() {
      return getCurrentIterator().hasNext();
    }

    @Override
    public java.util.Map.Entry<String, RavenJToken> next() {
      return getCurrentIterator().next();
    }

    @Override
    public void remove() {
      throw new IllegalStateException("Deleting elements in iterator is not implemneted!");
    }

  }

}
