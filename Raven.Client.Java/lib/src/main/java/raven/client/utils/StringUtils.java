package raven.client.utils;

public class StringUtils {
  public static boolean isNotNullOrEmpty(String value) {
    return value != null && !"".equals(value);
  }
}
