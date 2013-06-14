package raven.client.connection;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpStatus;
import org.apache.http.impl.cookie.DateParseException;
import org.apache.http.impl.cookie.DateUtils;

import raven.abstractions.data.Attachment;
import raven.abstractions.data.Constants;
import raven.abstractions.data.JsonDocument;
import raven.abstractions.data.JsonDocumentMetadata;
import raven.abstractions.extensions.MetadataExtensions;
import raven.abstractions.json.linq.JTokenType;
import raven.abstractions.json.linq.RavenJArray;
import raven.abstractions.json.linq.RavenJObject;
import raven.abstractions.json.linq.RavenJToken;

//TODO: finish me
public class SerializationHelper {

  public static JsonDocument deserializeJsonDocument(String docKey, RavenJToken responseJson, HttpJsonRequest jsonRequest) {
    RavenJObject jsonData = (RavenJObject) responseJson;
    RavenJObject meta = MetadataExtensions.filterHeaders(jsonRequest.getResponseHeaders());
    UUID etag = getEtag(jsonRequest.getResponseHeaders().get("ETag"));

    return new JsonDocument(jsonData, meta, docKey, jsonRequest.getResponseStatusCode() == HttpStatus.SC_NON_AUTHORITATIVE_INFORMATION, etag, getLastModifiedDate(jsonRequest));

  }

  private static UUID getEtag(String responseHeader) {
    return UUID.fromString(responseHeader);
  }

  private static Date getLastModifiedDate(HttpJsonRequest jsonRequest) {

    String ravenLastModified = jsonRequest.getResponseHeaders().get(Constants.RAVEN_LAST_MODIFIED);
    if (StringUtils.isNotEmpty(ravenLastModified)) {
      try {
        return new SimpleDateFormat(Constants.RAVEN_LAST_MODIFIED_DATE_FORAT).parse(ravenLastModified);
      } catch (ParseException e) {
        throw new IllegalArgumentException(e.getMessage(), e);
      }
    }
    String lastModified = jsonRequest.getResponseHeaders().get(Constants.LAST_MODIFIED);
    if (StringUtils.isNotEmpty(lastModified)) {
      try {
        return DateUtils.parseDate(lastModified);
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
    for (RavenJToken token : jArray) {
      if (token == null) {
        list.add(null);
        continue;
      }
      RavenJObject tokenObject = (RavenJObject) token;
      RavenJObject metadata = (RavenJObject) tokenObject.get("@metadata");
      tokenObject.remove("@metadata");

      String id = extract(metadata, "@id", "", String.class);
      UUID etag = extract(metadata, "@etag", null, UUID.class);

      //TODO: filter metadata headers
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

  public static JsonDocumentMetadata deserializeJsonDocumentMetadata(RavenJToken responseJson) {
    // TODO Auto-generated method stub
    return null;
  }
}
