package raven.client.json;

import org.codehaus.jackson.map.ObjectWriter;

import raven.client.common.extensions.JsonExtensions;

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

  public abstract boolean ensureCannotBeChangeAndEnableShapshotting();

  public abstract RavenJToken createSnapshot();

  protected static RavenJToken fromObjectInternal(Object o, ObjectWriter objectWritter) {
    //TODO: write customer writer (based on RavenJTokenWritter) JsonobjectWritter.writeValue(jgen, o);

    return null;

  }

  public static RavenJToken fromObject(Object o) {
    return fromObjectInternal(o, JsonExtensions.getDefaultObjectMapper().writer());
  }

}
