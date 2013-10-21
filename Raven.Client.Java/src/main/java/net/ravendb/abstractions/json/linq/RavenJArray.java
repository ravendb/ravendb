package net.ravendb.abstractions.json.linq;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import net.ravendb.abstractions.exceptions.JsonReaderException;
import net.ravendb.abstractions.exceptions.JsonWriterException;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;


/**
 * Represents a JSON array.
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

  public RavenJArray(Collection<?> content) {
    items = new ArrayList<>();
    if (content == null) {
      return;
    }

    if (content instanceof RavenJToken && ((RavenJToken) content).getType().equals(JTokenType.ARRAY)) {

      items.add((RavenJToken) content);
    } else {
      for (Object item : content) {
        if (item instanceof RavenJToken) {
          items.add((RavenJToken) item);
        } else {
          items.add(new RavenJValue(item));
        }
      }
    }
  }

  /**
   * Initializes a new instance of the {@link RavenJArray} class with the specified content.
   * @param content The contents of the array;
   */
  public RavenJArray(RavenJToken... content) {
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

  /**
   * Sets the {@link RavenJToken} at the specified index.
   * @param index
   * @param value
   */
  public void set(int index, RavenJToken value) {
    checkSnapshot();
    items.set(index, value);
  }

  private void checkSnapshot() {
    if (snapshot) {
      throw new IllegalArgumentException("Cannot modify a snapshot, this is probably a bug.");
    }
  }

  @Override
  public RavenJArray cloneToken() {
    return (RavenJArray) cloneTokenImpl(new RavenJArray());
  }

  @Override
  public boolean isSnapshot() {
    return snapshot;
  }

  public int size() {
    return items.size();
  }

  public static RavenJArray load(JsonParser parser) {
    try {
      if (parser.getCurrentToken() == null) {
        if (parser.nextToken() == null) {
          throw new JsonReaderException("Error reading RavenJToken from JsonParser");
        }
      }
      if (parser.getCurrentToken() != JsonToken.START_ARRAY) {
        throw new JsonReaderException(
          "Error reading RavenJArray from JsonParser. Current JsonReader item is not an array: "
            + parser.getCurrentToken());
      }
      // advance to next token
      parser.nextToken();

      RavenJArray ar = new RavenJArray();
      RavenJToken val = null;
      do {
        switch (parser.getCurrentToken()) {
          case END_ARRAY:
            return ar;
          case START_OBJECT:
            val = RavenJObject.load(parser);
            ar.add(val);
            break;
          case START_ARRAY:
            val = RavenJArray.load(parser);
            ar.add(val);
            break;
          default:
            val = RavenJValue.load(parser);
            ar.add(val);
            break;
        }
      } while (parser.nextToken() != null);

      throw new JsonReaderException("Error reading RavenJArray from JsonReader.");

    } catch (IOException e) {
      throw new JsonReaderException(e.getMessage(), e);
    }
  }

  /**
   * Load a {@link RavenJArray} from a string that contains JSON.
   * @param json A {@link String} that contains JSON.
   * @return A {@link RavenJArray} populated from the string that contains JSON.
   */
  public static RavenJArray parse(String json) {
    try {
      JsonParser jsonParser = new JsonFactory().createJsonParser(json);
      return load(jsonParser);
    } catch (IOException e) {
      throw new JsonReaderException(e.getMessage(), e);
    }
  }

  /* (non-Javadoc)
   * @see java.lang.Iterable#iterator()
   */
  @Override
  public Iterator<RavenJToken> iterator() {
    return items.iterator();
  }

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
  public RavenJArray createSnapshot() {
    if (snapshot == false)
      throw new IllegalStateException("Cannot create snapshot without previously calling EnsureSnapShot");

    return new RavenJArray(items);
  }

  @Override
  public void writeTo(JsonGenerator writer) {
    try {
      writer.writeStartArray();

      if (items != null) {
        for (RavenJToken token : items) {
          token.writeTo(writer);
        }
      }

      writer.writeEndArray();
    } catch (IOException e) {
      throw new JsonWriterException(e.getMessage(), e);
    }
  }

  @Override
  protected void addForCloning(String key, RavenJToken token) {
    add(token);
  }

  @Override
  public Iterable<RavenJToken> values() {
    return items;
  }

  @Override
  public <T> List<T> values(Class<T> clazz) {
    return Extensions.convert(clazz, items);
  }

}
