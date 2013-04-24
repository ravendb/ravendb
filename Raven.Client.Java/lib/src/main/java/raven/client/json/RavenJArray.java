package raven.client.json;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * Represents a JSON array.
 *
 */
public class RavenJArray extends RavenJToken implements Iterable<RavenJToken> {
  private boolean snapshot;

  private List<RavenJToken> items;

  /**
   * Initializes a new instance of the {@link RavenJArray} class.
   */
  public RavenJArray() {
    items = new ArrayList<>();
  }

  /**
   * Initializes a new instance of the {@link RavenJArray} class with the specified content.
   * @param content The contents of the array;
   */
  public RavenJArray(Collection<RavenJToken> content) {
    items =new ArrayList<>();
    if (content == null) {
      return ;
    }
    items.addAll(content);
  }

  public RavenJArray(RavenJToken[] content) {
    this(Arrays.asList(content));
  }

  /**
   * Gets the node type for this {@link RavenJArray}
   */
  @Override
  public JTokenType getType() {
    return JTokenType.ARRAY;
  }
  /**
   * Gets the {@link RavenJToken} at the specified index.
   * @param index
   * @return
   */
  public RavenJToken get(int index) {
    return items.get(index);
  }

  public void set(int index, RavenJToken value) {
    checkSnapshot();
    items.set(index, value);
  }

  private void checkSnapshot() {
    if(snapshot) {
      throw new IllegalArgumentException("Cannot modify a snapshot, this is probably a bug.");
    }
  }

  @Override
  public RavenJToken cloneToken() {
    return cloneTokenImpl(new RavenJArray());
  }


  @Override
  public boolean isSnapshot() {
    return snapshot;
  }

  public int getLength() {
    return items.size();
  }

  /**
   * @return the items
   */
  public List<RavenJToken> getItems() {
    return items;
  }

  /**
   * @param items the items to set
   */
  public void setItems(List<RavenJToken> items) {
    this.items = items;
  }

  /* (non-Javadoc)
   * @see java.lang.Iterable#iterator()
   */
  @Override
  public Iterator<RavenJToken> iterator() {
    return items.iterator();
  }





  //TODO: public new static RavenJArray Load(JsonReader reader)

  //TODO:  public new static RavenJArray Parse(string json)

  //TODO: public override void WriteTo(JsonWriter writer, params JsonConverter[] converters)

  public void add(RavenJToken token) {
    checkSnapshot();
    items.add(token);
  }

  public boolean remove(RavenJToken token) {
    checkSnapshot();
    return items.remove(token);
  }

  public void removeAt(int index) {
    checkSnapshot();
    items.remove(index);
  }

  /**
   * Inserts an item to the {@link List}   at the specified index.
   * @param index The zero-based index sat which item should be inserted
   * @param item The object to insert into the list.
   */
  public void insert(int index, RavenJToken item) {
    checkSnapshot();
    items.add(index, item);
  }

  /* (non-Javadoc)
   * @see raven.client.json.RavenJToken#ensureCannotBeChangeAndEnableShapshotting()
   */
  @Override
  public void ensureCannotBeChangeAndEnableShapshotting() {
    snapshot = true;
  }

  /* (non-Javadoc)
   * @see raven.client.json.RavenJToken#createSnapshot()
   */
  @Override
  public RavenJToken createSnapshot() {
    if (snapshot == false)
      throw new IllegalStateException("Cannot create snapshot without previously calling EnsureSnapShot");

    return new RavenJArray(items);
  }



}


