package raven.client.json;

import java.io.IOException;
import java.io.StringReader;
import java.text.ParseException;
import java.util.Map;
import java.util.Stack;

import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;

import raven.client.common.extensions.JsonExtensions;
import raven.client.json.lang.JsonReaderException;
import raven.client.json.lang.JsonWriterException;

/**
 * Represents an abstract JSON token.
 */
public abstract class RavenJToken {

  /**
   * Gets the node type for this {@link RavenJToken}
   * @return
   */
  public abstract JTokenType getType();

  /**
   * Clones this object
   * @return Cloned {@link RavenJToken}
   */
  public abstract RavenJToken cloneToken();

  public abstract boolean isSnapshot();

  public abstract void ensureCannotBeChangeAndEnableShapshotting();

  public abstract RavenJToken createSnapshot();

  protected static RavenJToken fromObjectInternal(Object o, ObjectMapper objectMapper) {
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

  /**
   * Creates a {@link RavenJToken} from an object.
   * @param o object
   * @return
   */
  public static RavenJToken fromObject(Object o) {
    return fromObjectInternal(o, JsonExtensions.getDefaultObjectMapper());
  }

  protected RavenJToken cloneTokenImpl(RavenJToken ravenJArray) {
    // TODO Auto-generated method stub
    return null;
  }

  //TODO :public override string ToString()

  public abstract void writeTo(JsonGenerator writer);

  public static RavenJToken readFrom(JsonParser parser) {
    try {
      if (parser.getCurrentToken() == null) {
        if (parser.nextToken() == null) {
          throw new JsonReaderException("Error reading RavenJToeken from JsonParser");
        }

        switch (parser.getCurrentToken()) {
        case START_OBJECT:
          return RavenJObject.load(parser);
        case START_ARRAY:
          return RavenJArray.load(parser);
        case VALUE_STRING:
          return new RavenJValue(parser.getCurrentToken().asString(), JTokenType.STRING);
        case VALUE_NUMBER_FLOAT:
          return new RavenJValue(parser.getDoubleValue(), JTokenType.FLOAT);
        case VALUE_NUMBER_INT:
          return new RavenJValue(parser.getIntValue(), JTokenType.INTEGER);
        case VALUE_FALSE:
        case VALUE_TRUE:
          return new RavenJValue(parser.getBooleanValue(), JTokenType.BOOLEAN);
        case VALUE_NULL:
          return new RavenJValue(null, JTokenType.NULL);
        }
      }
    } catch (IOException e) {
      throw new JsonReaderException("Error reading RavenJToeken from JsonParser");
    }
    throw new JsonReaderException("Error reading RavenJToeken from JsonParser");
  }

  /**
   * Load a {@link RavenJToken} from a string that contains JSON.
   * @param json
   * @return
   */
  public static RavenJToken parse(String json) throws JsonReaderException {
    try {
      JsonParser jsonParser = JsonExtensions.getDefaultJsonFactory().createJsonParser(json);
      return load(jsonParser);
    } catch (IOException e) {
      throw new JsonReaderException(e.getMessage(), e);
    }
  }

  //TODO: public static RavenJToken TryLoad(Stream stream)

  public static RavenJToken load(JsonParser parser) {
    return readFrom(parser);
  }

  //TODO: public virtual T Value<T>(string key) - not supported

  public static boolean DeepEquals(RavenJToken t1, RavenJToken t2) {
    return (t1 == t2 || t1 != null && t2 != null && t1.deepEquals(t2));
  }

  public static int deepHashCode(RavenJToken t) {
    return (t == null) ? 0 : t.deepHashCode();
  }

  public boolean deepEquals(RavenJToken other) {
    if (other == null)
      return false;

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

      if (curOtherReader == null && curThisReader == null)
        continue; // shouldn't happen, but we got an error report from a user about this
      if (curOtherReader == null || curThisReader == null)
        return false;

      if (curThisReader.getClass().equals(curOtherReader.getClass())) {
        switch (curOtherReader.getType()) {
        case ARRAY:
          RavenJArray selfArray = (RavenJArray) curThisReader;
          RavenJArray otherArray = (RavenJArray) curOtherReader;
          if (selfArray.getLength() != otherArray.getLength()) {
            return false;
          }
          for (int i = 0; i < selfArray.getLength(); i++) {
            thisStack.push(selfArray.get(i));
            otherStack.push(otherArray.get(i));
          }
          break;
        case OBJECT:
          RavenJObject selfObj = (RavenJObject) curOtherReader;
          RavenJObject otherObj = (RavenJObject) curOtherReader;

          if (selfObj.getCount() != otherObj.getCount()) {
            return false;
          }
          for (Map.Entry<String, RavenJToken> kvp : selfObj.getProperties()) {
            RavenJToken token;
            Tuple<Boolean, RavenJToken> returnedValue = otherObj.tryGetValue(kvp.getKey());
            if (!returnedValue.getItem1()) {
              return false;
            }
            token = returnedValue.getItem2();
            switch (kvp.getValue().getType()) {
            case ARRAY:
            case OBJECT:
              otherStack.push(token);
              thisStack.push(kvp.getValue());
              break;
            case BYTES:
              /* TODO:
               * var bytes = kvp.Value.Value<byte[]>();
                  byte[] tokenBytes = token.Type == JTokenType.String
                              ? Convert.FromBase64String(token.Value<string>())
                              : token.Value<byte[]>();
                      if (tokenBytes == null)
                          return false;
                  if (bytes.Length != tokenBytes.Length)
                    return false;

                  if (tokenBytes.Where((t, i) => t != bytes[i]).Any())
                  {
                    return false;
                  }

               */
              break;
            default:
              if (!kvp.getValue().deepEquals(token)) {
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

          /* TODO
        switch (curThisReader.Type)
        {
        case JTokenType.Guid:
          if (curOtherReader.Type != JTokenType.String)
            return false;

          if (curThisReader.Value<string>() != curOtherReader.Value<string>())
            return false;

          break;
        default:
          return false;
        }
        }*/
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
        for (int i = 0; i < arr.getLength(); i++) {
          stack.push(Tuple.create(cur.getItem1() ^ (i * 397), arr.get(i)));
        }
      } else if (cur.getItem2().getType() == JTokenType.OBJECT) {
        RavenJObject selfObj = (RavenJObject) cur.getItem2();
        for (Map.Entry<String, RavenJToken> kvp : selfObj.getProperties()) {
          stack.push(Tuple.create(cur.getItem1() ^ (397 * kvp.getKey().hashCode()), kvp.getValue()));
        }
      } else {
        ret ^= cur.getItem1() ^ (cur.getItem2().deepHashCode() * 397);
      }
    }

    return ret;
  }
}
