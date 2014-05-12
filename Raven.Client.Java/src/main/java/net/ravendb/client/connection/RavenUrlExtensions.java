package net.ravendb.client.connection;

import java.util.Map;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;

import net.ravendb.abstractions.connection.OperationCredentials;
import net.ravendb.abstractions.data.HttpMethods;
import net.ravendb.client.connection.implementation.HttpJsonRequest;
import net.ravendb.client.document.DocumentConvention;
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
    return url;
  }

  public static HttpJsonRequest toJsonRequest(String url, ServerClient requestor, OperationCredentials credentials, DocumentConvention convention) {
    return requestor.getJsonRequestFactory().createHttpJsonRequest(new CreateHttpJsonRequestParams(requestor, url, HttpMethods.GET, null, credentials, convention));
  }

  public static HttpJsonRequest toJsonRequest(String url, ServerClient requestor, OperationCredentials credentials, DocumentConvention convention, Map<String, String> operationsHeaders, HttpMethods method) {
    return requestor.getJsonRequestFactory().createHttpJsonRequest(new CreateHttpJsonRequestParams(requestor, url, method, null, credentials, convention)).addOperationHeaders(operationsHeaders);
  }



}
