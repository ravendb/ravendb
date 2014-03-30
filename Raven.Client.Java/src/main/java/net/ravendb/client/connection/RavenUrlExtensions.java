package net.ravendb.client.connection;

import java.util.UUID;

import org.apache.commons.lang.StringUtils;

import net.ravendb.client.utils.UrlUtils;


public class RavenUrlExtensions {

  public static String forDatabase(String url, String database) {
    if (StringUtils.isNotEmpty(database) && !url.contains("/databases/")){
      return url + "/databases/" + database;
    }
    return url;
  }

  public static String indexes(String url, String index) {
    return url + "/indexes/" + index;
  }

  public static String indexDefinition(String url, String index) {
    return url + "/indexes/" + index + "?definition=yes";
  }

  public static String tranformer(String url, String transformer) {
    return url + "/transformers/" + transformer;
  }

  public static String indexNames(String url, int start, int pageSize) {
    return url + "/indexes/?namesOnly=true&start=" + start + "&pageSize=" + pageSize;
  }

  public static String stats(String url) {
    return url + "/stats";
  }

  public static String adminStats(String url) {
    return url + "/admin/stats";
  }

  public static String replicationInfo(String url) {
    return url + "/replication/info";
  }

  public static String lastReplicatedEtagFor(String destinationUrl, String sourceUrl) {
    return destinationUrl + "/replication/lastEtag?from=" + UrlUtils.escapeDataString(sourceUrl);
  }

  public static String databases(String url, int pageSize, int start) {
    String databases = url + "/databases?pageSize=" + pageSize;
    return start > 0 ? databases + "&start=" + start : databases;
  }

  public static String terms(String url, String index, String field, String fromValue, int pageSize) {
    return url + "/terms/" + index + "?field=" + field + "&fromValue=" + fromValue + "&pageSize=" + pageSize;
  }

  public static String docs(String url, int start, int pageSize) {
    return url + "/docs/?start=" + start + "&pageSize=" + pageSize;
  }

  public static String queries(String url) {
    return url + "/queries/";
  }

  public static String noCache(String url) {
    return url + (url.contains("?") ? "&" : "?") + "noCache=" + UUID.randomUUID().hashCode();
  }

}
