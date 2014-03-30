package net.ravendb.client.document;

import java.util.HashMap;
import java.util.Map;

/**
 *  Helper class for reflection operations
 */
public class ReflectionUtil {

  private static Map<Class<?>, String> fullnameCache = new HashMap<>();

  /**
   * Note: we can't fetch generic types information in Java - hence we are limited to simple getName on class object
   * @param entityType
   * @return
   */
  public static String getFullNameWithoutVersionInformation(Class<?> entityType) {
    if (fullnameCache.containsKey(entityType)) {
      return fullnameCache.get(entityType);
    }

    String fullName = entityType.getName();
    fullnameCache.put(entityType, fullName);
    return fullName;
  }
}
