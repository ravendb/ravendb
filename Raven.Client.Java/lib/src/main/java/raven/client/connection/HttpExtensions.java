package raven.client.connection;


import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpResponse;

import raven.abstractions.data.Etag;
import raven.abstractions.data.GetResponse;
import raven.client.connection.implementation.HttpJsonRequest;

public class HttpExtensions {
  public static Etag getEtagHeader(HttpJsonRequest request) {
    return etagHeaderToEtag(request.getResponseHeaders().get("ETag"));
  }

  public static Etag etagHeaderToEtag(String responseHeader) {
    if (StringUtils.isEmpty(responseHeader)) {
      throw new IllegalArgumentException("Response didn't had an ETag header.");
    }
    if (responseHeader.charAt(0) == '"') {
      return Etag.parse(responseHeader.substring(1, responseHeader.length() - 1));
    }
    return Etag.parse(responseHeader);
  }

  public static Etag getEtagHeader(HttpResponse httpResponse) {
    return etagHeaderToEtag(httpResponse.getFirstHeader("ETag").getValue());
  }

  public static Etag getEtagHeader(GetResponse response) {
    return etagHeaderToEtag(response.getHeaders().get("ETag"));
  }
}
