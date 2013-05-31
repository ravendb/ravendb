package raven.client.connection;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.util.DateParseException;
import org.apache.commons.httpclient.util.DateUtil;

import raven.abstractions.data.Attachment;
import raven.client.data.Constants;
import raven.client.json.JTokenType;
import raven.client.json.JsonDocument;
import raven.client.json.RavenJArray;
import raven.client.json.RavenJObject;
import raven.client.json.RavenJToken;
import raven.client.utils.StringUtils;

public class SerializationHelper {
  public final static Set<String> HEADERS_TO_IGNORE_CLIENT = new HashSet<>();
  static {
    //TODO: move to metadata extensions
    // Raven internal headers
    HEADERS_TO_IGNORE_CLIENT.add("Raven-Server-Build");
    HEADERS_TO_IGNORE_CLIENT.add("Raven-Client-Version");
    HEADERS_TO_IGNORE_CLIENT.add("Non-Authoritative-Information");
    HEADERS_TO_IGNORE_CLIENT.add("Raven-Timer-Request");
    HEADERS_TO_IGNORE_CLIENT.add("Raven-Authenticated-User");
    HEADERS_TO_IGNORE_CLIENT.add("Raven-Last-Modified");
    HEADERS_TO_IGNORE_CLIENT.add("Has-Api-Key");

    // COTS
    HEADERS_TO_IGNORE_CLIENT.add("Access-Control-Allow-Origin");
    HEADERS_TO_IGNORE_CLIENT.add("Access-Control-Max-Age");
    HEADERS_TO_IGNORE_CLIENT.add("Access-Control-Allow-Methods");
    HEADERS_TO_IGNORE_CLIENT.add("Access-Control-Request-Headers");
    HEADERS_TO_IGNORE_CLIENT.add("Access-Control-Allow-Headers");

    //proxy
    HEADERS_TO_IGNORE_CLIENT.add("Reverse-Via");
    HEADERS_TO_IGNORE_CLIENT.add("Persistent-Auth");
    HEADERS_TO_IGNORE_CLIENT.add("Allow");
    HEADERS_TO_IGNORE_CLIENT.add("Content-Disposition");
    HEADERS_TO_IGNORE_CLIENT.add("Content-Encoding");
    HEADERS_TO_IGNORE_CLIENT.add("Content-Language");
    HEADERS_TO_IGNORE_CLIENT.add("Content-Location");
    HEADERS_TO_IGNORE_CLIENT.add("Content-MD5");
    HEADERS_TO_IGNORE_CLIENT.add("Content-Range");
    HEADERS_TO_IGNORE_CLIENT.add("Content-Type");
    HEADERS_TO_IGNORE_CLIENT.add("Expires");
    // ignoring this header, we handle this internally
    HEADERS_TO_IGNORE_CLIENT.add("Last-Modified");
    // Ignoring this header, since it may
    // very well change due to things like encoding,
    // adding metadata, etc
    HEADERS_TO_IGNORE_CLIENT.add("Content-Length");
    // Special things to ignore
    HEADERS_TO_IGNORE_CLIENT.add("Keep-Alive");
    HEADERS_TO_IGNORE_CLIENT.add("X-Powered-By");
    HEADERS_TO_IGNORE_CLIENT.add("X-AspNet-Version");
    HEADERS_TO_IGNORE_CLIENT.add("X-Requested-With");
    HEADERS_TO_IGNORE_CLIENT.add("X-SourceFiles");
    // Request headers
    HEADERS_TO_IGNORE_CLIENT.add("Accept-Charset");
    HEADERS_TO_IGNORE_CLIENT.add("Accept-Encoding");
    HEADERS_TO_IGNORE_CLIENT.add("Accept");
    HEADERS_TO_IGNORE_CLIENT.add("Accept-Language");
    HEADERS_TO_IGNORE_CLIENT.add("Authorization");
    HEADERS_TO_IGNORE_CLIENT.add("Cookie");
    HEADERS_TO_IGNORE_CLIENT.add("Expect");
    HEADERS_TO_IGNORE_CLIENT.add("From");
    HEADERS_TO_IGNORE_CLIENT.add("Host");
    HEADERS_TO_IGNORE_CLIENT.add("If-Match");
    HEADERS_TO_IGNORE_CLIENT.add("If-Modified-Since");
    HEADERS_TO_IGNORE_CLIENT.add("If-None-Match");
    HEADERS_TO_IGNORE_CLIENT.add("If-Range");
    HEADERS_TO_IGNORE_CLIENT.add("If-Unmodified-Since");
    HEADERS_TO_IGNORE_CLIENT.add("Max-Forwards");
    HEADERS_TO_IGNORE_CLIENT.add("Referer");
    HEADERS_TO_IGNORE_CLIENT.add("TE");
    HEADERS_TO_IGNORE_CLIENT.add("User-Agent");
    //Response headers
    HEADERS_TO_IGNORE_CLIENT.add("Accept-Ranges");
    HEADERS_TO_IGNORE_CLIENT.add("Age");
    HEADERS_TO_IGNORE_CLIENT.add("Allow");
    HEADERS_TO_IGNORE_CLIENT.add("ETag");
    HEADERS_TO_IGNORE_CLIENT.add("Location");
    HEADERS_TO_IGNORE_CLIENT.add("Retry-After");
    HEADERS_TO_IGNORE_CLIENT.add("Server");
    HEADERS_TO_IGNORE_CLIENT.add("Set-Cookie2");
    HEADERS_TO_IGNORE_CLIENT.add("Set-Cookie");
    HEADERS_TO_IGNORE_CLIENT.add("Vary");
    HEADERS_TO_IGNORE_CLIENT.add("Www-Authenticate");
    // General
    HEADERS_TO_IGNORE_CLIENT.add("Cache-Control");
    HEADERS_TO_IGNORE_CLIENT.add("Connection");
    HEADERS_TO_IGNORE_CLIENT.add("Date");
    HEADERS_TO_IGNORE_CLIENT.add("Pragma");
    HEADERS_TO_IGNORE_CLIENT.add("Trailer");
    HEADERS_TO_IGNORE_CLIENT.add("Transfer-Encoding");
    HEADERS_TO_IGNORE_CLIENT.add("Upgrade");
    HEADERS_TO_IGNORE_CLIENT.add("Via");
    HEADERS_TO_IGNORE_CLIENT.add("Warning");

    // IIS Application Request Routing Module
    HEADERS_TO_IGNORE_CLIENT.add("X-ARR-LOG-ID");
    HEADERS_TO_IGNORE_CLIENT.add("X-ARR-SSL");
    HEADERS_TO_IGNORE_CLIENT.add("X-Forwarded-For");
    HEADERS_TO_IGNORE_CLIENT.add("X-Original-URL");
  }


  public static JsonDocument deserializeJsonDocument(String docKey, RavenJToken responseJson, HttpJsonRequest jsonRequest) {
    RavenJObject jsonData = (RavenJObject) responseJson;
    RavenJObject meta = getMetadata(jsonRequest);
    UUID etag = getEtag(jsonRequest.getResponseHeader("ETag"));

    return new JsonDocument(jsonData, meta, docKey,
        jsonRequest.getResponseCode() == HttpStatus.SC_NON_AUTHORITATIVE_INFORMATION, etag, getLastModifiedDate(jsonRequest));

  }

  private static RavenJObject getMetadata(HttpJsonRequest jsonRequest) {
    RavenJObject metadata = new RavenJObject();
    for (Header header: jsonRequest.getResponseHeaders()) {
      if (header.getName().startsWith("Temp")) {
        continue;
      }
      if (header.getName().equals(Constants.DOCUMENT_ID_FIELD_NAME)) {
        continue;
      }
      if (HEADERS_TO_IGNORE_CLIENT.contains(header.getName())) {
        continue;
      }
      metadata.add(header.getName(), RavenJToken.fromObject(header.getValue()));
    }

    return metadata;
  }

  private static UUID getEtag(String responseHeader) {
    return UUID.fromString(responseHeader);
  }

  private static Date getLastModifiedDate(HttpJsonRequest jsonRequest) {

    String ravenLastModified = jsonRequest.getResponseHeader(Constants.RAVEN_LAST_MODIFIED);
    if (StringUtils.isNotNullOrEmpty(ravenLastModified)) {
      try {
        return new SimpleDateFormat(Constants.RAVEN_LAST_MODIFIED_DATE_FORAT).parse(ravenLastModified);
      } catch (ParseException e) {
        throw new IllegalArgumentException(e.getMessage(), e);
      }
    }
    String lastModified = jsonRequest.getResponseHeader(Constants.LAST_MODIFIED);
    if (StringUtils.isNotNullOrEmpty(lastModified)) {
      try {
        return DateUtil.parseDate(lastModified);
      } catch (DateParseException e) {
        throw new IllegalArgumentException(e.getMessage(), e);
      }
    }

    return null;
  }

  private static <T> T extract(RavenJObject metadata, String key, T defaultValue, Class<T> expectedClass) {
    if (metadata == null || !metadata.containsKey(key)) {
      return defaultValue;
    }
    if (JTokenType.ARRAY == metadata.get(key).getType()) {
      return defaultValue;
    }
    return metadata.value(expectedClass, key);

  }

  public static List<JsonDocument> ravenJObjectsToJsonDocuments(RavenJToken responseJson) {
    List<JsonDocument> list = new ArrayList<>();

    RavenJArray jArray = (RavenJArray) responseJson;
    for (RavenJToken token :jArray) {
      if (token == null) {
        list.add(null);
        continue;
      }
      RavenJObject tokenObject = (RavenJObject) token;
      RavenJObject metadata = (RavenJObject) tokenObject.get("@metadata");
      tokenObject.remove("@metadata");


      String id = extract(metadata, "@id", "", String.class);
      UUID etag = extract(metadata, "@etag", null, UUID.class);

      Date lastModified = null; //TODO: set me!
      boolean nonAuthoritativeInformation = extract(metadata, "Non-Authoritative-Information", Boolean.FALSE, Boolean.class);


      list.add(new JsonDocument(tokenObject, metadata, id, nonAuthoritativeInformation, etag, lastModified));
    }
    return list;
  }

  public static List<Attachment> deserializeAttachements(RavenJToken responseJson) {
    // TODO Auto-generated method stub
    return null;
  }
}
