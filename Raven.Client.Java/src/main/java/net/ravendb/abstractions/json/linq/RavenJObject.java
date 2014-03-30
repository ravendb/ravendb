package net.ravendb.abstractions.json.linq;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import net.ravendb.abstractions.basic.Reference;
import net.ravendb.abstractions.exceptions.JsonReaderException;
import net.ravendb.abstractions.exceptions.JsonWriterException;
import net.ravendb.abstractions.extensions.JsonExtensions;
import net.ravendb.abstractions.json.RavenJsonTextReader;

import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.map.ObjectMapper;

import com.google.common.base.Defaults;


public class RavenJObject extends RavenJToken implements Iterable<Entry<String, RavenJToken>> {

  private final Comparator<String> comparer;
  /**
   * Creates a {@link RavenJObject} from an object.
   * @param o The object that will be used to create {@link RavenJObject}
   * @return A {@link RavenJObject} with the values of the specified object.
   */
  public static RavenJObject fromObject(Object o) {
    return fromObject(o, JsonExtensions.createDefaultJsonSerializer());
  }

  /**
   * Creates a {@link RavenJObject} from an object.
   * @param o The object that will be used to create {@link RavenJObject}
   * @param objectMapper The {@link ObjectMapper} that will be used to read the object.
   * @return {@link RavenJObject} with the values of the specified object.
   */
  public static RavenJObject fromObject(Object o, ObjectMapper objectMapper) {
    RavenJToken token = fromObjectInternal(o, objectMapper);

    if (token != null && token.getType() != JTokenType.OBJECT)
      throw new IllegalArgumentException("Object serialized to " + token.getType()
        + ". RavenJObject instance expected.");

    return (RavenJObject) token;
  }

  public static RavenJObject load(JsonParser parser) {
    try {
      if (parser.getCurrentToken() == null) {
        if (parser.nextToken() == null) {
          throw new JsonReaderException("Error reading RavenJToken from JsonParser");
        }
      }
      if (parser.getCurrentToken() != JsonToken.START_OBJECT) {
        throw new JsonReaderException(
          "Error reading RavenJObject from JsonParser. Current JsonReader item is not an object: "
            + parser.getCurrentToken());
      }
      if (parser.nextToken() == null) {
        throw new JsonReaderException("Unexpected end of json object");
      }

      String propName = null;
      RavenJObject o = new RavenJObject();
      do {

        switch (parser.getCurrentToken()) {
          case FIELD_NAME:
            propName = parser.getText();
            break;
          case END_OBJECT:
            return o;
          case START_OBJECT:
            if (StringUtils.isNotEmpty(propName)) {
              RavenJObject val = RavenJObject.load(parser);
              o.set(propName, val);
              propName = null;
            } else {
              throw new JsonReaderException("The JsonReader should not be on a token of type "
                + parser.getCurrentToken());
            }
            break;
          case START_ARRAY:
            if (StringUtils.isNotEmpty(propName)) {
              RavenJArray val = RavenJArray.load(parser);
              o.set(propName, val);
              propName = null;
            } else {
              throw new JsonReaderException("The JsonReader should not be on a token of type "
                + parser.getCurrentToken());
            }
            break;
          default:
            if (StringUtils.isNotEmpty(propName)) {
              RavenJValue val = (RavenJValue) RavenJToken.load(parser);
              o.set(propName, val);
              propName = null;
            } else {
              throw new JsonReaderException("The JsonReader should not be on a token of type "
                + parser.getCurrentToken());
            }
            break;

        }
      } while (parser.nextToken() != null);

      throw new JsonReaderException("Error reading RavenJObject from JsonReader.");

    } catch (IOException e) {
      throw new JsonWriterException(e.getMessage(), e);
    }
  }

  /**
   * Loads {@link RavenJObject} from a string that contains JSON.
   * @param json A {@link String} that contains JSON.
   * @return A {@link RavenJObject} populated from the string that contains JSON.
   */
  public static RavenJObject parse(String json) {
    try {
      JsonParser jsonParser = new RavenJsonTextReader().createJsonParser(json);
      return load(jsonParser);
    } catch (IOException e) {
      throw new JsonReaderException(e.getMessage(), e);
    }
  }

  private DictionaryWithParentSnapshot properties;

  public RavenJObject() {
    this((Comparator<String>)null);
  }

  public RavenJObject(Comparator<String> comparator) {
    this.comparer = comparator;
    this.properties = new DictionaryWithParentSnapshot(comparator);
  }


  public RavenJObject(DictionaryWithParentSnapshot snapshot) {
    properties = snapshot;
    this.comparer = null;
  }

  public RavenJObject(RavenJObject other) {
    properties = new DictionaryWithParentSnapshot(other.comparer);
    for (Map.Entry<String, RavenJToken> kv : other.getProperties().entrySet()) {
      properties.put(kv.getKey(), kv.getValue());
    }
    this.comparer = other.comparer;
  }

  public void add(String propertyName, Object value) {
    properties.put(propertyName, RavenJToken.fromObject(value));
  }

  public void add(String propertyName, RavenJToken token) {
    properties.put(propertyName, token);
  }

  @Override
  protected void addForCloning(String key, RavenJToken token) {
    properties.put(key, token);
  }

  @Override
  public RavenJObject cloneToken() {
    return (RavenJObject) cloneTokenImpl(new RavenJObject());
  }

  public boolean containsKey(String key) {
    return properties.containsKey(key);
  }

  @Override
  public RavenJObject createSnapshot() {
    return new RavenJObject(properties.createSnapshot());
  }

  @Override
  public void ensureCannotBeChangeAndEnableShapshotting() {
    properties.ensureSnapshot();
  }

  public void ensureSnapshot(String msg) {
    properties.ensureSnapshot(msg);
  }

  public RavenJToken get(String propertyName) {
    return properties.get(propertyName);
  }

  public int getCount() {
    return properties.size();
  }

  public Set<String> getKeys() {
    return properties.keySet();
  }

  /**
   * @return the properties
   */
  public DictionaryWithParentSnapshot getProperties() {
    return properties;
  }

  @Override
  public JTokenType getType() {
    return JTokenType.OBJECT;
  }

  @Override
  public boolean isSnapshot() {
    return properties.isSnapshot();
  }

  public RavenJObject withCaseInsensitivePropertyNames() {
    DictionaryWithParentSnapshot props = new DictionaryWithParentSnapshot(String.CASE_INSENSITIVE_ORDER);
    for (Map.Entry<String, RavenJToken> property: properties) {
      props.put(property.getKey(), property.getValue());
    }
    return new RavenJObject(props);
  }

  public boolean remove(String propertyName) {
    return properties.remove(propertyName) != null;
  }

  public void set(String propertyName, RavenJToken value) {
    properties.put(propertyName, value);
  }

  /**
   * Writes this token to a {@link JsonGenerator}
   * @param writer A {@link JsonGenerator} into which this method will write.
   */
  @Override
  public void writeTo(JsonGenerator writer) {
    try {
      writer.writeStartObject();

      if (properties != null) {
        for (String key : properties.keySet()) {
          writer.writeFieldName(key);
          RavenJToken value = properties.get(key);
          if (value == null) {
            writer.writeNull();
          } else {
            value.writeTo(writer);
          }
        }
      }
      writer.writeEndObject();
    } catch (IOException e) {
      throw new JsonWriterException(e.getMessage(), e);
    }

  }

  /* (non-Javadoc)
   * @see raven.client.json.RavenJToken#value(java.lang.Class, java.lang.String)
   */
  @SuppressWarnings("null")
  @Override
  public <T> T value(Class<T> clazz, String key) {
    if (!containsKey(key)) {
      return Defaults.defaultValue(clazz);
    }
    RavenJToken ravenJToken = get(key);
    if (ravenJToken != null) {
      return Extensions.convert(clazz, ravenJToken);
    }
    throw new IllegalArgumentException("Unsupported conversion. From:" + ravenJToken.getType() + " to "
      + clazz.getCanonicalName());
  }

  @Override
  public Iterable<RavenJToken> values() {
    return properties.values();
  }

  @Override
  public <T> List<T> values(Class<T> clazz) {
    return Extensions.convert(clazz, properties.values());
  }

  @Override
  public Iterator<Entry<String, RavenJToken>> iterator() {
    return properties.iterator();
  }

  public boolean tryGetValue(String name, Reference<RavenJToken> value) {
    return properties.tryGetValue(name, value);
  }

}
