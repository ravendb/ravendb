package raven.client.utils;

public class StringUtils {
  public static boolean isNotNullOrEmpty(String value) {
    return value != null && !"".equals(value);
  }

  public static String repeat(String str, int repeat) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < repeat; i++) {
      sb.append(str);
    }
    return sb.toString();
  }

  public static Object defaultIfNull(String input, String defaultInput) {
    return (input != null) ? input : defaultInput;
  }

  public static boolean isNullOrEmpty(String value) {
    return value == null || "".equals(value);
  }
}
