package net.ravendb.client.document;

import java.io.IOException;
import java.util.List;

import net.ravendb.abstractions.data.Facet;
import net.ravendb.abstractions.data.FacetResults;
import net.ravendb.abstractions.data.GetRequest;
import net.ravendb.abstractions.data.GetResponse;
import net.ravendb.abstractions.data.IndexQuery;
import net.ravendb.abstractions.data.QueryResult;
import net.ravendb.abstractions.extensions.JsonExtensions;
import net.ravendb.abstractions.json.linq.RavenJObject;
import net.ravendb.client.document.batches.ILazyOperation;
import net.ravendb.client.utils.UrlUtils;
import net.ravendb.imports.json.JsonConvert;

import org.apache.http.HttpStatus;


public class LazyFacetsOperation implements ILazyOperation {

  private final String index;
  private final List<Facet> facets;
  private final String facetSetupDoc;
  private QueryResult queryResult;
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

  @Override
  public GetRequest createRequest() {
    String addition = null;
    if (facetSetupDoc != null) {
      addition = "facetDoc="  + facetSetupDoc;
    } else {
      addition = "facets=" + UrlUtils.escapeDataString(JsonConvert.serializeObject(facets));
    }

    GetRequest getRequest = new GetRequest();
    getRequest.setUrl("/facets/" + index);
    getRequest.setQuery(String.format("%s&facetStart=%d&facetPageSize=%d&%s", query.getMinimalQueryString(), start, pageSize, addition));
    return getRequest;
  }

  @Override
  public Object getResult() {
    return result;
  }

  @Override
  public boolean isRequiresRetry() {
    return requiresRetry;
  }

  @Override
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

  @Override
  public QueryResult getQueryResult() {
    return queryResult;
  }


  public void setQueryResult(QueryResult queryResult) {
    this.queryResult = queryResult;
  }

  @Override
  public AutoCloseable enterContext() {
    return null;
  }

}
