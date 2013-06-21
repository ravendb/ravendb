package raven.client.connection;


import org.apache.commons.lang.StringUtils;

import raven.abstractions.data.Etag;

public class HttpExtensions {
  public static Etag getEtagHeader(HttpJsonRequest request) {
    return etagHeaderToEtag(request.getResponseHeaders().get("ETag"));
  }

  private static Etag etagHeaderToEtag(String responseHeader) {
    if (StringUtils.isEmpty(responseHeader)) {
      throw new IllegalArgumentException("Response didn't had an ETag header.");
    }
    if (responseHeader.charAt(0) == '"') {
      return Etag.fromString(responseHeader.substring(1, responseHeader.length() - 2));
    }
    return Etag.fromString(responseHeader);
  }
}
