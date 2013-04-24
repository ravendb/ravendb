package raven.client.json;

import java.io.IOException;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import raven.client.common.extensions.JsonExtensions;
import raven.client.json.lang.JsonWriterException;

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

  public static RavenJToken fromObject(Object o) {
    return fromObjectInternal(o, JsonExtensions.getDefaultObjectMapper());
  }


  protected RavenJToken cloneTokenImpl(RavenJArray ravenJArray) {
    // TODO Auto-generated method stub
    return null;
  }


  //TODO :public override string ToString()

//TODO:  public abstract void writeTo(JsonWriter writer);

}
