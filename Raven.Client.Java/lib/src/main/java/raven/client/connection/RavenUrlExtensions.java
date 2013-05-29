package raven.client.connection;

import java.util.UUID;

public class RavenUrlExtensions {
  public static String noCache(String url) {
    return url +  (url.contains("?") ? "&":"?" ) + "noCache=" + UUID.randomUUID().hashCode();
  }

  public static String databases(String url, int pageSize, int start) {
    String databases = url + "/databases?pageSize=" + pageSize;
    return start > 0 ? databases + "&start=" + start : databases;
  }
}
