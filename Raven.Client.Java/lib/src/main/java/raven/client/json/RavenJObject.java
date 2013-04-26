package raven.client.json;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.map.ObjectMapper;

import raven.client.common.extensions.JsonExtensions;
import raven.client.json.lang.JsonReaderException;
import raven.client.json.lang.JsonWriterException;
import raven.client.utils.StringUtils;

public class RavenJObject extends RavenJToken   {

  /**
   * Creates a {@link RavenJObject} from an object.
   * @param o The object that will be used to create {@link RavenJObject}
   * @return A {@link RavenJObject} with the values of the specified object.
   */
  public static RavenJObject fromObject(Object o) {
    return fromObject(o, JsonExtensions.getDefaultObjectMapper());
  }

  /**
   * Creates a {@link RavenJObject} from an object.
   * @param o The object that will be used to create {@link RavenJObject}
   * @param objectMapper The {@link ObjectMapper} that will be used to read the object.
   * @return {@link RavenJObject} with the values of the specified object.
   */
  private static RavenJObject fromObject(Object o, ObjectMapper objectMapper) {
    RavenJToken token = fromObjectInternal(o, objectMapper);

    if (token != null && token.getType() != JTokenType.OBJECT)
      throw new IllegalArgumentException("Object serialized to " + token.getType() + ". RavenJObject instance expected.");

    return (RavenJObject)token;
  }

  public static RavenJObject load(JsonParser parser) {
    try {
      JsonToken currentToken = parser.getCurrentToken();
      if (currentToken == null) {
        if (parser.nextToken() == null) {
          throw new JsonReaderException("Error reading RavenJToken from JsonParser");
        }
      }
      if (currentToken != JsonToken.START_OBJECT) {
        throw new JsonReaderException("Error reading RavenJObject from JsonParser. Current JsonReader item is not an object: " + parser.getCurrentToken());
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
          if (StringUtils.isNotNullOrEmpty(propName)) {
            RavenJObject val =  RavenJObject.load(parser);
            o.set(propName, val);
            propName = null;
          } else {
            throw new JsonReaderException("The JsonReader should not be on a token of type " + parser.getCurrentToken());
          }
          break;
        case START_ARRAY:
          if (StringUtils.isNotNullOrEmpty(propName)) {
            RavenJArray val = RavenJArray.load(parser);
            o.set(propName, val);
            propName = null;
          } else {
            throw new JsonReaderException("The JsonReader should not be on a token of type " + parser.getCurrentToken());
          }
          break;
          default:
            if (StringUtils.isNotNullOrEmpty(propName)) {
              RavenJValue val = (RavenJValue) RavenJToken.load(parser);
              o.set(propName, val);
              propName = null;
            } else {
              throw new JsonReaderException("The JsonReader should not be on a token of type " + parser.getCurrentToken());
            }
            break;

        }
      } while (parser.nextToken() != null);

      throw new JsonReaderException("Error reading RavenJObject from JsonReader.");

    } catch (IOException e) {
      throw new JsonWriterException(e.getMessage(),e);
    }
  }

  /**
   * Loads {@link RavenJObject} from a string that contains JSON.
   * @param json A {@link String} that contains JSON.
   * @return A {@link RavenJObject} populated from the string that contains JSON.
   */
  public static RavenJObject parse(String json) {
    try {
      JsonParser jsonParser = JsonExtensions.getDefaultJsonFactory().createJsonParser(json);
      return load(jsonParser);
    } catch (IOException e) {
      throw new JsonReaderException(e.getMessage(), e);
    }
  }

  private MapWithParentSnapshot properties = new MapWithParentSnapshot();

  public RavenJObject() {
    // empty by design
  }

  public RavenJObject(MapWithParentSnapshot snapshot) {
    properties = snapshot;
  }

  public RavenJObject(RavenJObject other) {
    properties = new MapWithParentSnapshot();
    for (Map.Entry<String, RavenJToken> kv : other.getProperties().entrySet()) {
      properties.put(kv.getKey(), kv.getValue());
    }
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
  public RavenJToken createSnapshot() {
    return new RavenJObject(properties.createSnapshot());
  }

  public boolean deepEquals(RavenJToken other) {
    if (!(other instanceof RavenJObject)) {
      return false;
    }
    return super.deepEquals(other);
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
  public MapWithParentSnapshot getProperties() {
    return properties;
  }

  /* (non-Javadoc)
   * @see raven.client.json.RavenJToken#getType()
   */
  @Override
  public JTokenType getType() {
    return JTokenType.OBJECT;
  }


  @Override
  public boolean isSnapshot() {
    return properties.isSnapshot();
  }

  public boolean remove(String propertyName) {
    return properties.remove(propertyName) != null;
  }

  public void set(String propertyName, RavenJToken value) {
    properties.put(propertyName, value);
  }

  /**
   * @param properties the properties to set
   */
  public void setProperties(MapWithParentSnapshot properties) {
    this.properties = properties;
  }

  public Tuple<Boolean, RavenJToken> tryGetValue(String key) {
    // TODO Auto-generated method stub
    return null;
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
        for (String key: properties.keySet()) {
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


}
