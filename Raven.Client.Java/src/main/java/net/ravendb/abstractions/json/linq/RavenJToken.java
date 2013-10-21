package net.ravendb.abstractions.json.linq;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.List;
import java.util.Stack;

import net.ravendb.abstractions.basic.Tuple;
import net.ravendb.abstractions.exceptions.JsonReaderException;
import net.ravendb.abstractions.exceptions.JsonWriterException;
import net.ravendb.abstractions.extensions.JsonExtensions;

import org.codehaus.jackson.FormatSchema;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;


/**
 * Represents an abstract JSON token.
 */
public abstract class RavenJToken {

  public static boolean deepEquals(RavenJToken t1, RavenJToken t2) {
    return (t1 == t2 || t1 != null && t2 != null && t1.deepEquals(t2));
  }

  public static int deepHashCode(RavenJToken t) {
    return (t == null) ? 0 : t.deepHashCode();
  }

  /**
   * Creates a {@link RavenJToken} from an object.
   * @param o object
   * @return
   */
  public static RavenJToken fromObject(Object o) {
    return fromObjectInternal(o, JsonExtensions.createDefaultJsonSerializer());
  }

  @SuppressWarnings("unused")
  private static RavenJToken fromObject(Object o, ObjectMapper objectMapper) {
    return fromObjectInternal(o, objectMapper);
  }

  protected static RavenJToken fromObjectInternal(Object o, ObjectMapper objectMapper) {
    if (o == null) {
      return RavenJValue.getNull();
    }
    if (o instanceof RavenJToken) {
      return (RavenJToken) o;
    }

    RavenJTokenWriter ravenJTokenWriter = new RavenJTokenWriter();
    try {
      objectMapper.writerWithType(o.getClass()).writeValue(ravenJTokenWriter, o);
    } catch (IOException e) {
      throw new JsonWriterException(e.getMessage(), e);
    }
    return ravenJTokenWriter.getToken();
  }

  public static RavenJToken load(JsonParser parser) {
    return readFrom(parser);
  }

  /**
   * Load a {@link RavenJToken} from a string that contains JSON.
   * @param json
   * @return
   */
  public static RavenJToken parse(String json) throws JsonReaderException {
    try {
      JsonParser jsonParser = new JsonFactory().createJsonParser(json);
      return load(jsonParser);
    } catch (IOException e) {
      throw new JsonReaderException(e.getMessage(), e);
    }
  }

  /**
   * Load a {@link RavenJToken} from a string that contains JSON.
   * @param json
   * @return
   */
  public static RavenJToken tryLoad(InputStream json) throws JsonReaderException {
    try {
      JsonParser jsonParser = new JsonFactory().createJsonParser(json);
      if (!jsonParser.hasCurrentToken()) {
        if (jsonParser.nextToken() == null) {
          return null;
        }
      }

      return load(jsonParser);
    } catch (IOException e) {
      throw new JsonReaderException(e.getMessage(), e);
    }
  }

  public static RavenJToken readFrom(JsonParser parser) {
    try {
      if (parser.getCurrentToken() == null) {
        if (parser.nextToken() == null) {
          throw new JsonReaderException("Error reading RavenJToken from JsonParser");
        }
      }

      switch (parser.getCurrentToken()) {
        case START_OBJECT:
          return RavenJObject.load(parser);
        case START_ARRAY:
          return RavenJArray.load(parser);
        case VALUE_STRING:
          return new RavenJValue(parser.getText(), JTokenType.STRING);
        case VALUE_NUMBER_FLOAT:
          return new RavenJValue(parser.getNumberValue(), JTokenType.FLOAT);
        case VALUE_NUMBER_INT:
          return new RavenJValue(parser.getNumberValue(), JTokenType.INTEGER);
        case VALUE_FALSE:
        case VALUE_TRUE:
          return new RavenJValue(parser.getBooleanValue(), JTokenType.BOOLEAN);
        case VALUE_NULL:
          return new RavenJValue(null, JTokenType.NULL);
      }
    } catch (IOException e) {
      throw new JsonReaderException("Error reading RavenJToken from JsonParser" + e.getMessage(), e);
    }
    throw new JsonReaderException("Error reading RavenJToken from JsonParser");
  }

  protected void addForCloning(String key, RavenJToken token) {
    //empty by design
  }

  /**
   * Clones this object
   * @return Cloned {@link RavenJToken}
   */
  public abstract RavenJToken cloneToken();

  @Override
  public String toString() {
    try {
      StringWriter stringWriter = new StringWriter();
      JsonGenerator jsonGenerator = new JsonFactory().createJsonGenerator(stringWriter);
      writeTo(jsonGenerator);
      jsonGenerator.close();
      return stringWriter.toString();
    } catch (IOException e) {
      throw new JsonWriterException(e.getMessage(), e);
    }
  }

  public String toString(FormatSchema schema, Object[] converters) {
    //implement me
    return toString();
  }

  protected RavenJToken cloneTokenImpl(RavenJToken newObject) {
    Stack<RavenJToken> readingStack = new Stack<>();
    Stack<RavenJToken> writingStack = new Stack<>();

    writingStack.push(newObject);
    readingStack.push(this);

    while (!readingStack.isEmpty()) {
      RavenJToken curReader = readingStack.pop();
      RavenJToken curObject = writingStack.pop();

      if (curReader instanceof RavenJObject) {
        RavenJObject ravenJObject = (RavenJObject) curReader;
        for (String key : ravenJObject.getProperties().keySet()) {
          RavenJToken value = ravenJObject.get(key);
          if (value == null || value.getType() == JTokenType.NULL) {
            curObject.addForCloning(key, RavenJValue.getNull());
            continue;
          }
          if (value instanceof RavenJValue) {
            curObject.addForCloning(key, value.cloneToken());
            continue;
          }

          RavenJToken newVal = (value instanceof RavenJArray) ? new RavenJArray() : new RavenJObject();
          curObject.addForCloning(key, newVal);

          writingStack.push(newVal);
          readingStack.push(value);
        }
      } else if (curObject instanceof RavenJArray) {
        RavenJArray ravenJArray = (RavenJArray) curReader;
        for (RavenJToken token : ravenJArray) {
          if (token == null || token.getType() == JTokenType.NULL) {
            curObject.addForCloning(null, null);
            continue;
          }
          if (token instanceof RavenJValue) {
            curObject.addForCloning(null, token.cloneToken());
            continue;
          }
          RavenJToken newVal = (token instanceof RavenJArray) ? new RavenJArray() : new RavenJObject();
          curObject.addForCloning(null, newVal);

          writingStack.push(newVal);
          readingStack.push(token);
        }
      } else {
        throw new IllegalStateException("Unexpected token type:" + curReader.getType());
      }

    }

    return newObject;
  }

  @Override
  public int hashCode() {
    return deepHashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (this == obj) {
      return true;
    }
    if (!this.getClass().equals(obj.getClass())) {
      return false;
    }
    return deepEquals((RavenJToken) obj);
  }

  public abstract RavenJToken createSnapshot();

  public boolean deepEquals(RavenJToken other) {
    if (other == null) {
      return false;
    }
    if (this == other) {
      return true;
    }

    if (!getClass().equals(other.getClass())) {
      return false;
    }

    Stack<RavenJToken> otherStack = new Stack<>();
    Stack<RavenJToken> thisStack = new Stack<>();

    thisStack.push(this);
    otherStack.push(other);

    while (!otherStack.isEmpty()) {
      RavenJToken curOtherReader = otherStack.pop();
      RavenJToken curThisReader = thisStack.pop();

      if (curOtherReader == null && curThisReader == null) continue; // shouldn't happen, but we got an error report from a user about this
      if (curOtherReader == null || curThisReader == null) return false;

      if (curThisReader.getClass().equals(curOtherReader.getClass())) {
        switch (curOtherReader.getType()) {
          case ARRAY:
            RavenJArray selfArray = (RavenJArray) curThisReader;
            RavenJArray otherArray = (RavenJArray) curOtherReader;
            if (selfArray.size() != otherArray.size()) {
              return false;
            }
            for (int i = 0; i < selfArray.size(); i++) {
              thisStack.push(selfArray.get(i));
              otherStack.push(otherArray.get(i));
            }
            break;
          case OBJECT:
            RavenJObject selfObj = (RavenJObject) curThisReader;
            RavenJObject otherObj = (RavenJObject) curOtherReader;

            if (selfObj.getCount() != otherObj.getCount()) {
              return false;
            }
            for (String key : selfObj.getProperties().keySet()) {
              RavenJToken value = selfObj.get(key);
              RavenJToken token;
              RavenJToken returnedValue = otherObj.get(key);
              if (returnedValue == null) {
                return false;
              }
              token = returnedValue;
              switch (value.getType()) {
                case ARRAY:
                case OBJECT:
                  otherStack.push(token);
                  thisStack.push(value);
                  break;
                default:
                  if (!value.deepEquals(token)) {
                    return false;
                  }
                  break;
              }
            }
            break;
          default:
            if (!curOtherReader.deepEquals(curThisReader)) {
              return false;
            }
            break;
        }
      }
    }

    return true;
  }

  public int deepHashCode() {
    Stack<Tuple<Integer, RavenJToken>> stack = new Stack<>();
    int ret = 0;

    stack.push(Tuple.create(0, this));
    while (!stack.isEmpty()) {
      Tuple<Integer, RavenJToken> cur = stack.pop();
      if (cur.getItem2().getType() == JTokenType.ARRAY) {
        RavenJArray arr = (RavenJArray) cur.getItem2();
        for (int i = 0; i < arr.size(); i++) {
          stack.push(Tuple.create(cur.getItem1() ^ (i * 397), arr.get(i)));
        }
      } else if (cur.getItem2().getType() == JTokenType.OBJECT) {
        RavenJObject selfObj = (RavenJObject) cur.getItem2();
        for (String key : selfObj.getProperties().keySet()) {
          RavenJToken value = selfObj.get(key);
          stack.push(Tuple.create(cur.getItem1() ^ (397 * value.hashCode()), value));
        }
      } else {
        ret ^= cur.getItem1() ^ (cur.getItem2().deepHashCode() * 397);
      }
    }

    return ret;
  }

  public RavenJToken selectToken(String path) {
    return selectToken(path, false);
  }

  public RavenJToken selectToken(String path, boolean errorWhenNoMatch) {
    RavenJPath p = new RavenJPath(path);
    return p.evaluate(this, errorWhenNoMatch);
  }

  public RavenJToken selectToken(RavenJPath path) {
    return selectToken(path, false);
  }

  public RavenJToken selectToken(RavenJPath path, boolean errorWhenNoMatch) {
    return path.evaluate(this, errorWhenNoMatch);
  }

  public Iterable<RavenJToken> values() {
    throw new UnsupportedOperationException();
  }

  public <T> List<T> values(Class<T> clazz) {
    throw new UnsupportedOperationException();
  }

  public abstract void ensureCannotBeChangeAndEnableShapshotting();

  /**
   * Gets the node type for this {@link RavenJToken}
   * @return
   */
  public abstract JTokenType getType();

  public abstract boolean isSnapshot();

  public abstract void writeTo(JsonGenerator writer);

  public <T> T value(Class<T> clazz, String key) {
    throw new IllegalStateException("Unsupported operation!");
  }

  public <T> T value(Class<T> clazz) {
    return Extensions.value(clazz, this);
  }
}
