package net.ravendb.client.util;

import net.ravendb.abstractions.json.linq.RavenJObject;

public class Types {
  public static boolean isEntityType(Class<?> clazz) {
    return !Object.class.equals(clazz) && !RavenJObject.class.equals(clazz);
  }
}
