package raven.client.document;

import java.io.IOException;
import java.util.List;

import org.apache.http.HttpStatus;

import raven.abstractions.data.Facet;
import raven.abstractions.data.FacetResults;
import raven.abstractions.data.GetRequest;
import raven.abstractions.data.GetResponse;
import raven.abstractions.data.IndexQuery;
import raven.abstractions.extensions.JsonExtensions;
import raven.abstractions.json.linq.RavenJObject;
import raven.client.document.batches.ILazyOperation;
import raven.client.utils.UrlUtils;
import raven.imports.json.JsonConvert;

public class LazyFacetsOperation implements ILazyOperation {

  private final String index;
  private final List<Facet> facets;
  private final String facetSetupDoc;
  private final IndexQuery query;
  private final int start;
  private final Integer pageSize;

  private Object result;
  private boolean requiresRetry;


  public LazyFacetsOperation(String index, String facetSetupDoc, IndexQuery query) {
    this(index, facetSetupDoc, query, 0, null);
  }

  public LazyFacetsOperation(String index, String facetSetupDoc, IndexQuery query, int start) {
    this(index, facetSetupDoc, query, start, null);
  }

  public LazyFacetsOperation(String index, String facetSetupDoc, IndexQuery query, int start, Integer pageSize) {
    this.index = index;
    this.facetSetupDoc = facetSetupDoc;
    this.query = query;
    this.start = start;
    this.pageSize = pageSize;
    this.facets = null;
  }

  public LazyFacetsOperation(String index, List<Facet> facets, IndexQuery query) {
    this(index, facets, query, 0, null);
  }

  public LazyFacetsOperation(String index, List<Facet> facets, IndexQuery query, int start) {
    this(index, facets, query, start, null);
  }

  public LazyFacetsOperation(String index, List<Facet> facets, IndexQuery query, int start, Integer pageSize) {
    this.index = index;
    this.facets = facets;
    this.query = query;
    this.start = start;
    this.pageSize = pageSize;
    this.facetSetupDoc = null;
  }

  public GetRequest createRequest() {
    String addition = null;
    if (facetSetupDoc != null) {
      addition = "facetDoc="  + facetSetupDoc;
    } else {
      addition = "facets=" + UrlUtils.escapeDataString(JsonConvert.serializeObject(facets));
    }

    GetRequest getRequest = new GetRequest();
    getRequest.setUrl("/facets/" + index);
    getRequest.setQuery(String.format("&query=%s&facetStart=%d&facetPageSize=%d&%s", query.getQuery(), start, pageSize, addition));
    return getRequest;
  }

  public Object getResult() {
    return result;
  }

  public boolean isRequiresRetry() {
    return requiresRetry;
  }

  public void handleResponse(GetResponse response) {
    if (response.getStatus() != HttpStatus.SC_OK && response.getStatus() != HttpStatus.SC_NOT_MODIFIED) {
      throw new IllegalStateException("Got an unexpected response code for the request: " + response.getStatus() + "\n" + response.getResult());
    }
    try {
      RavenJObject result =  (RavenJObject) response.getResult();
      this.result = JsonExtensions.createDefaultJsonSerializer().readValue(result.toString(), FacetResults.class);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public AutoCloseable enterContext() {
    return null;
  }

}
