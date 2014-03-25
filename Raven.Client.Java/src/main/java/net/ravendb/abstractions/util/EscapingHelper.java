package net.ravendb.abstractions.util;

import net.ravendb.client.utils.UrlUtils;


public class EscapingHelper {
  //hide constructor
  private EscapingHelper() {
  }

  public static String escapeLongDataString(String data) {
      int limit = 65519;

      if (data.length() <= limit) {
        return UrlUtils.escapeDataString(data);
      }

      StringBuilder result = new StringBuilder();

      int loops = data.length() / limit;

      for (int i = 0; i <= loops; i++) {
          if (i < loops) {
              result.append(UrlUtils.escapeDataString(data.substring(limit * i, limit)));
          } else {
              result.append(UrlUtils.escapeDataString(data.substring(limit * i)));
          }
      }

      return result.toString();
  }
}
