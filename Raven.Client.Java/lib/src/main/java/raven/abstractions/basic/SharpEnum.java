package raven.abstractions.basic;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.google.common.base.CaseFormat;

/**
 * Utility class for inter-language enum conversion
 */
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

  public static String value(Enum<?> enumValue) {
    return CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, enumValue.name());
  }

  public static String value(EnumSet<? extends Enum<?>> aggregationOperation) {
    List<String> tokens = new ArrayList<>();
    for (Enum<?> e : aggregationOperation) {
      tokens.add(value(e));
    }
    return StringUtils.join(tokens, ",");
  }

}
