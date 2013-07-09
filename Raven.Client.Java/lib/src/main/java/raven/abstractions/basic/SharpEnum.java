package raven.abstractions.basic;

import com.google.common.base.CaseFormat;

public class SharpEnum {

  public static <T extends Enum<T>>  T fromValue(String v, Class<T> class1) {
    v = CaseFormat.UPPER_CAMEL.to(CaseFormat.UPPER_UNDERSCORE, v);
    for (T e: class1.getEnumConstants()) {
      if (e.name().equals(v)) {
        return e;
      }
    }
    throw new IllegalArgumentException("Unable to find enum for:" + v);
  }

}
