package raven.client.connection;

import java.util.UUID;

import org.apache.commons.lang.StringUtils;

public class HttpExtensions {
  public static UUID getEtagHeader(HttpJsonRequest request) {
    return etagHeaderToEtag(request.getResponseHeaders().get("ETag"));
  }

  private static UUID etagHeaderToEtag(String responseHeader) {
    if (StringUtils.isEmpty(responseHeader)) {
      throw new IllegalArgumentException("Response didn't had an ETag header.");
    }
    if (responseHeader.charAt(0) == '"') {
      return UUID.fromString(responseHeader.substring(1, responseHeader.length() - 2));
    }
    return UUID.fromString(responseHeader);
  }
}
