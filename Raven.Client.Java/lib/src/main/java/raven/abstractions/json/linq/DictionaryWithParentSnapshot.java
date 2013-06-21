package raven.abstractions.json.linq;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import raven.abstractions.basic.Holder;

public class DictionaryWithParentSnapshot implements Map<String, RavenJToken>, Iterable<Map.Entry<String, RavenJToken>> {

  private static final RavenJToken DELETED_MARKER = new RavenJValue("*DeletedMarker*", JTokenType.NULL);

  private final DictionaryWithParentSnapshot parentSnapshot;
  private int count;
  private Map<String, RavenJToken> localChanges = new HashMap<>();
  private String snapshotMsg;
  private boolean snapshot;

  public Map<String, RavenJToken> getLocalChanges() {
    return localChanges;
  }

  public DictionaryWithParentSnapshot() {
    this(null);
  }

  public DictionaryWithParentSnapshot(DictionaryWithParentSnapshot previous) {
    this.parentSnapshot = previous;
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
    if (containsKey(key)) {
      throw new IllegalArgumentException("An item with the same key has already been added: " + key);
    }
    count++;
    localChanges.put(key, value);
    return value;
  }

  /* (non-Javadoc)
   * @see java.util.Map#containsKey(java.lang.Object)
   */
  @Override
  public boolean containsKey(Object keyObject) {
    String key = (String) keyObject;
    RavenJToken token;
    if (localChanges.containsKey(key)) {
      token = localChanges.get(key);
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
    if (localChanges.isEmpty()) {
      if (parentSnapshot != null) {
        return parentSnapshot.keySet();
      }
      return new HashSet<>();
    }

    Set<String> ret = new HashSet<>();
    if (parentSnapshot != null) {
      for (String key : parentSnapshot.keySet()) {
        if (!localChanges.containsKey(key)) {
          ret.add(key);
        }
      }
    }
    for (String key : localChanges.keySet()) {
      if (localChanges.containsKey(key) && localChanges.get(key) != DELETED_MARKER) {
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

    if (!localChanges.containsKey(key)) {
      if (parentHasIt && parentToken != DELETED_MARKER) {
        localChanges.put(key, DELETED_MARKER);
        count--;
        return parentToken;
      }
      return null;
    }
    RavenJToken token = localChanges.get(key);
    if (token == DELETED_MARKER) {
      return null;
    }
    count--;
    localChanges.put(key, DELETED_MARKER);
    return token;
  }

  /* (non-Javadoc)
   * @see java.util.Map#get(java.lang.Object)
   */
  @Override
  public RavenJToken get(Object keyObject) {
    String key = (String) keyObject;
    if (localChanges.containsKey(key)) {
      RavenJToken unsafeVal = localChanges.get(key);
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
    throw new IllegalStateException("Not implemeneted. Use keySet instead.");
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

  public boolean tryGetValue(String key, Holder<RavenJToken> value) {
    value.value = null;
    Holder<RavenJToken> unsafeVal = new Holder<RavenJToken>();
    if (localChanges != null && localChanges.containsKey(key)) {
      unsafeVal.value = localChanges.get(key);
      if (DELETED_MARKER.equals(unsafeVal.value)) return false;

      value = unsafeVal;
      return true;
    }

    if (parentSnapshot == null ||
      !parentSnapshot.tryGetValue(key, unsafeVal) ||
      DELETED_MARKER.equals(unsafeVal.value)) return false;

    if (isSnapshot() && unsafeVal.value != null) {
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
        localIterator = localChanges.entrySet().iterator();
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
