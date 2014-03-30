package net.ravendb.abstractions.extensions;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import net.ravendb.abstractions.data.Constants;
import net.ravendb.abstractions.json.linq.RavenJArray;
import net.ravendb.abstractions.json.linq.RavenJObject;
import net.ravendb.abstractions.json.linq.RavenJToken;
import net.ravendb.abstractions.json.linq.RavenJValue;
import net.ravendb.abstractions.util.NetDateFormat;
import net.ravendb.client.utils.UrlUtils;


/**
 * Extensions for handling metadata
 */
public class MetadataExtensions {
  public final static Set<String> HEADERS_TO_IGNORE_CLIENT = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
  static {
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

  /**
   * Filters headers from unwanted headers
   * @param self
   * @return
   */
  public static RavenJObject filterHeaders(RavenJObject self) {
    if (self == null) {
      return null;
    }

    RavenJObject metadata = new RavenJObject(String.CASE_INSENSITIVE_ORDER);
    for (Entry<String, RavenJToken> header : self) {
      if (header.getKey().startsWith("Temp")) {
        continue;
      }
      if (header.getKey().equals(Constants.DOCUMENT_ID_FIELD_NAME)) {
        continue;
      }
      if (HEADERS_TO_IGNORE_CLIENT.contains(header.getKey())) {
        continue;
      }
      String headerName = captureHeaderName(header.getKey());
      metadata.add(headerName, header.getValue());
    }
    return metadata;
  }

  public static RavenJObject filterHeadersAttachment(Map<String, String> self) {
    RavenJObject filteredHeaders = filterHeaders(self);
    if (self.get("Content-Type") != null) {
      filteredHeaders.add("Content-Type", RavenJValue.fromObject(self.get("Content-Type")));
    }
    return filteredHeaders;
  }

  /**
   * Filters the headers from unwanted headers
   * @param headers
   * @return
   */
  public static RavenJObject filterHeaders(Map<String, String> headers) {
    RavenJObject metadata = new RavenJObject(String.CASE_INSENSITIVE_ORDER);
    for (Entry<String, String> header : headers.entrySet()) {
      if (header.getKey().startsWith("Temp")) {
        continue;
      }
      if (header.getKey().equals(Constants.DOCUMENT_ID_FIELD_NAME)) {
        continue;
      }
      if (HEADERS_TO_IGNORE_CLIENT.contains(header.getKey())) {
        continue;
      }
      Set<String> values = new HashSet<>();
      values.add(header.getValue());
      String headerName = captureHeaderName(header.getKey());
      if (values.size() == 1) {
        metadata.add(headerName, getValue(values.iterator().next()));
      } else {
        List<RavenJToken> headerValues = new ArrayList<>();
        for (String value : values) {
          headerValues.add(getValue(value));
        }
        metadata.add(headerName, new RavenJArray(headerValues.subList(0, Math.min(15, headerValues.size()))));
      }
    }
    return metadata;
  }

  private static String captureHeaderName(String header) {
    boolean lastWasDash = true;
    StringBuilder sb = new StringBuilder(header.length());

    for (int i = 0; i < header.length(); i++) {
      char ch = header.charAt(i);
      sb.append(lastWasDash ? Character.toUpperCase(ch) : ch);
      lastWasDash = ch == '-';

    }
    return sb.toString();
  }

  private static RavenJToken getValue(String val) {
    if (val.startsWith("{")) {
      return RavenJObject.parse(val);
    }
    if (val.startsWith("[")) {
      return RavenJArray.parse(val);
    }

    NetDateFormat netDateFormat = new NetDateFormat();
    try {
      Date date = netDateFormat.parse(val);
      return new RavenJValue(date);
    } catch (ParseException | IllegalArgumentException e) {
      //ignore
    }
    return new RavenJValue(UrlUtils.unescapeDataString(val));
  }

}
