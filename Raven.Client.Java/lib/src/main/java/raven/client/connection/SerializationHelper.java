package raven.client.connection;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpStatus;
import org.apache.http.impl.cookie.DateParseException;
import org.apache.http.impl.cookie.DateUtils;

import raven.abstractions.data.Attachment;
import raven.abstractions.data.Constants;
import raven.abstractions.data.Etag;
import raven.abstractions.data.JsonDocument;
import raven.abstractions.data.JsonDocumentMetadata;
import raven.abstractions.data.QueryResult;
import raven.abstractions.extensions.MetadataExtensions;
import raven.abstractions.json.linq.JTokenType;
import raven.abstractions.json.linq.RavenJArray;
import raven.abstractions.json.linq.RavenJObject;
import raven.abstractions.json.linq.RavenJToken;

//TODO: finish me
public class SerializationHelper {


  private static Date getLastModifiedDate(Map<String, String> headers) {

    String ravenLastModified = headers.get(Constants.RAVEN_LAST_MODIFIED);
    if (StringUtils.isNotEmpty(ravenLastModified)) {
      try {
        return new SimpleDateFormat(Constants.RAVEN_LAST_MODIFIED_DATE_FORMAT).parse(ravenLastModified);
      } catch (ParseException e) {
        throw new IllegalArgumentException(e.getMessage(), e);
      }
    }
    String lastModified = headers.get(Constants.LAST_MODIFIED);
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
      Etag etag = extract(metadata, "@etag", null, Etag.class);

      //TODO: filter metadata headers
      Date lastModified = null; //TODO: set me!
      boolean nonAuthoritativeInformation = extract(metadata, "Non-Authoritative-Information", Boolean.FALSE, Boolean.class);

      list.add(new JsonDocument(tokenObject, metadata, id, nonAuthoritativeInformation, etag, lastModified));
    }
    return list;
  }

  /**
   * Java only
   * @param responseJson
   * @return
   */
  public static List<Attachment> deserializeAttachements(RavenJToken responseJson) {
    // TODO Auto-generated method stub
    return null;
  }

  public static JsonDocumentMetadata deserializeJsonDocumentMetadata(RavenJToken responseJson) {
    // TODO Auto-generated method stub
    return null;
  }

  public static JsonDocumentMetadata deserializeJsonDocumentMetadata(String docKey, Map<String, String> headers, int responseStatusCode) {
    RavenJObject meta = MetadataExtensions.filterHeaders(headers);
    Etag etag = HttpExtensions.etagHeaderToEtag(headers.get("ETag"));
    JsonDocumentMetadata result =  new JsonDocumentMetadata();
    result.setEtag(etag);
    result.setKey(docKey);
    result.setLastModified(getLastModifiedDate(headers));
    result.setMetadata(meta);
    result.setMonAuthoritativeInformation(responseStatusCode == HttpStatus.SC_NON_AUTHORITATIVE_INFORMATION);
    return result;
  }

  public static JsonDocument ravenJObjectToJsonDocument(RavenJObject doc) {
    RavenJObject metadata = (RavenJObject) doc.get("@metadata");
    doc.remove("@metadata");
    String key = extract(metadata, "@id", "", String.class);

    //TODO: var lastModified = GetLastModified(metadata);
    Date lastModified = new Date(); //TODO delete me!

    Etag etag = extract(metadata, "@etag", Etag.empty(), Etag.class);
    boolean nai = extract(metadata, "Non-Authoritative-Information", false, Boolean.class);
    JsonDocument jsonDocument = new JsonDocument(doc, MetadataExtensions.filterHeaders(metadata), key, nai, etag, lastModified);
    jsonDocument.setTempIndexScore(metadata == null ? null : metadata.value(Float.class, Constants.TEMPORARY_SCORE_VALUE));

    return jsonDocument;
  }

  public static JsonDocument deserializeJsonDocument(String docKey, RavenJToken responseJson, Map<String, String> headers, int responseStatusCode) {
    /* TODO: update me! */
    RavenJObject jsonData = (RavenJObject) responseJson;

    RavenJObject meta = MetadataExtensions.filterHeaders(headers);
    Etag etag = HttpExtensions.etagHeaderToEtag(headers.get("ETag"));

    return new JsonDocument(jsonData, meta, docKey, responseStatusCode == HttpStatus.SC_NON_AUTHORITATIVE_INFORMATION, etag, getLastModifiedDate(headers));
    //return null;

  }

  public static JsonDocument toJsonDocument(RavenJObject r) {
    // TODO Auto-generated method stub
    return null;
  }

  public static QueryResult toQueryResult(RavenJObject json, Etag etagHeader, String tempRequestTime) {
    QueryResult result = new QueryResult();
    result.setStale(json.value(Boolean.class, "IsStale"));
    result.setIndexTimestamp(json.value(Date.class, "IndexTimestamp"));
    result.setIndexEtag(etagHeader);
    result.setResults(json.value(RavenJArray.class, "Results").values(RavenJObject.class));
    result.setIncludes(json.value(RavenJArray.class, "Includes").values(RavenJObject.class));
    result.setTotalResults(json.value(Integer.class, "TotalResults"));
    result.setIndexName(json.value(String.class, "IndexName"));
    result.setSkippedResults(json.value(Integer.class, "SkippedResults"));
//  TODO:        Highlightings = (json.Value<RavenJObject>("Highlightings") ?? new RavenJObject())
//            .JsonDeserialization<Dictionary<string, Dictionary<string, string[]>>>()

    if (json.containsKey("NonAuthoritativeInformation")) {
      result.setNonAuthoritativeInformation(json.value(Boolean.class, "NonAuthoritativeInformation"));
    }
    if (json.containsKey("DurationMilliseconds")) {
      result.setDurationMiliseconds(json.value(Long.class, "DurationMilliseconds"));
    }
    if (StringUtils.isNotEmpty(tempRequestTime)) {
      result.setDurationMiliseconds(Long.valueOf(tempRequestTime));
    }
    return result;
  }

}
