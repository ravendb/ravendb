package net.ravendb.client.document.sessionoperations;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import net.ravendb.abstractions.basic.Tuple;
import net.ravendb.abstractions.closure.Function0;
import net.ravendb.abstractions.data.JsonDocument;
import net.ravendb.abstractions.data.MultiLoadResult;
import net.ravendb.abstractions.json.linq.RavenJObject;
import net.ravendb.abstractions.logging.ILog;
import net.ravendb.abstractions.logging.LogManager;
import net.ravendb.client.connection.SerializationHelper;
import net.ravendb.client.document.InMemoryDocumentSessionOperations;

import org.apache.commons.lang.StringUtils;

import com.google.common.base.Defaults;


public class MultiLoadOperation {
  private static final ILog log = LogManager.getCurrentClassLogger();

  private final InMemoryDocumentSessionOperations sessionOperations;
  protected Function0<AutoCloseable> disableAllCaching;
  private final String[] ids;
  private final Tuple<String, Class<?>>[] includes;
  boolean firstRequest = true;
  JsonDocument[] results;
  JsonDocument[] includeResults;

  private long spStart;

  public MultiLoadOperation(InMemoryDocumentSessionOperations sessionOperations, Function0<AutoCloseable> disableAllCaching, String[] ids, Tuple<String, Class<?>>[] includes) {
    this.sessionOperations = sessionOperations;
    this.disableAllCaching = disableAllCaching;
    this.ids = ids;
    this.includes = includes;
  }

  public void logOperation() {
    if (ids == null) {
      return;
    }
    log.debug("Bulk loading ids [%s] from %s", StringUtils.join(ids, ", "), sessionOperations.getStoreIdentifier());
  }

  public AutoCloseable enterMultiLoadContext() {
    if (firstRequest == false) { // if this is a repeated request, we mustn't use the cached result, but have to re-query the server
      return disableAllCaching.apply();
    }
    spStart = new Date().getTime();
    return null;
  }

  public boolean setResult(MultiLoadResult multiLoadResult) {
    firstRequest = false;
    includeResults = SerializationHelper.ravenJObjectsToJsonDocuments(multiLoadResult.getIncludes()).toArray(new JsonDocument[0]);
    results = SerializationHelper.ravenJObjectsToJsonDocuments(multiLoadResult.getResults()).toArray(new JsonDocument[0]);

    if (!sessionOperations.isAllowNonAuthoritativeInformation()) {
      return false;
    }
    for (JsonDocument doc : results) {
      if (doc != null && !Boolean.TRUE.equals(doc.getNonAuthoritativeInformation())) {
        return false;
      }
    }
    if ( (new Date().getTime() - spStart ) < sessionOperations.getNonAuthoritativeInformationTimeout()) {
      return false;
    }
    return true;
  }

  public <T> T[] complete(Class<T> clazz) {
    for (int i = 0; i < includeResults.length; i++) {
      JsonDocument include = includeResults[i];
      Class<?> entityType = (this.includes.length > i) ? this.includes[i].getItem2() : Object.class;
      sessionOperations.trackEntity(entityType, include);
    }

    JsonDocument[] selectedResults = selectResults();

    T[] finalResults = (T[]) Array.newInstance(clazz, selectedResults.length);
    for (int i = 0; i < selectedResults.length; i++) {
      finalResults[i] = (T) (selectedResults[i] == null ? Defaults.defaultValue(clazz) : sessionOperations.trackEntity(clazz, selectedResults[i]));
    }

    for (int i = 0; i < finalResults.length; i++) {
      if (finalResults[i] == null) {
        sessionOperations.registerMissing(ids[i]);
      }
    }

    List<String> includePaths = null;
    if (this.includes != null) {
      includePaths = new ArrayList<>();
      for (Tuple<String, Class<?>> pair : this.includes) {
        includePaths.add(pair.getItem1());
      }
    }

    List<RavenJObject> missingInc = new ArrayList<>();
    for (JsonDocument doc: results) {
      if (doc != null) {
        missingInc.add(doc.getDataAsJson());
      }
    }
    sessionOperations.registerMissingIncludes(missingInc, includePaths);

    return finalResults;
  }

  private JsonDocument[] selectResults() {
    if (ids == null) {
      return results;
    }
    JsonDocument[] finalResult = new JsonDocument[ids.length];
    for (int i = 0; i < ids.length; i++) {
      String id = ids[i];
      for (JsonDocument doc: results) {
        if (doc != null && StringUtils.equalsIgnoreCase(id, doc.getMetadata().value(String.class, "@id"))) {
          finalResult[i] = doc;
          break;
        }
      }
    }

    return finalResult;
  }

}

