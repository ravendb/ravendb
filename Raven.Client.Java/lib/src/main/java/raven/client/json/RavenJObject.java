package raven.client.json;

import org.codehaus.jackson.map.ObjectMapper;

import raven.client.common.extensions.JsonExtensions;

public class RavenJObject extends RavenJToken {

  /* (non-Javadoc)
   * @see raven.client.json.RavenJToken#getType()
   */
  @Override
  public JTokenType getType() {
    return JTokenType.OBJECT;
  }

  public static RavenJObject fromObject(Object o) {
    return fromObject(o, JsonExtensions.getDefaultObjectMapper());
  }

  private static RavenJObject fromObject(Object o, ObjectMapper objectMapper) {
    RavenJToken token = fromObjectInternal(o, objectMapper);

    if (token != null && token.getType() != JTokenType.OBJECT)
      throw new IllegalArgumentException("Object serialized to " + token.getType() + ". RavenJObject instance expected.");

    return (RavenJObject)token;
  }

  public void set(String key, RavenJToken value) {
    //TODO: impl me
  }

  @Override
  public RavenJToken cloneToken() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean isSnapshot() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public void ensureCannotBeChangeAndEnableShapshotting() {
    // TODO Auto-generated method stub
  }

  @Override
  public RavenJToken createSnapshot() {
    // TODO Auto-generated method stub
    return null;
  }

}
