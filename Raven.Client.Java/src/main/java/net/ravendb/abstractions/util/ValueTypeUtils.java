package net.ravendb.abstractions.util;

import java.util.UUID;

public class ValueTypeUtils {
    public static boolean isValueType(Class<?> clazz) {
      return clazz.isPrimitive() ||
          UUID.class.equals(clazz) ||
          Number.class.isAssignableFrom(clazz);
    }
}
