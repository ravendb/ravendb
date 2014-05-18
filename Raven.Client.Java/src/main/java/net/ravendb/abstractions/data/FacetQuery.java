package net.ravendb.abstractions.data;

import java.util.List;


public class FacetQuery {
  private String indexName;
  private IndexQuery query;
  private String facetSetupDoc;
  private List<Facet> facets;
  private int pageStart;
  private Integer pageSize;

  public String getIndexName() {
    return indexName;
  }

  public void setIndexName(String indexName) {
    this.indexName = indexName;
  }

  public IndexQuery getQuery() {
    return query;
  }

  public void setQuery(IndexQuery query) {
    this.query = query;
  }

  public String getFacetSetupDoc() {
    return facetSetupDoc;
  }

  public void setFacetSetupDoc(String facetSetupDoc) {
    this.facetSetupDoc = facetSetupDoc;
  }

  public List<Facet> getFacets() {
    return facets;
  }

  public void setFacets(List<Facet> facets) {
    this.facets = facets;
  }

  public int getPageStart() {
    return pageStart;
  }

  public void setPageStart(int pageStart) {
    this.pageStart = pageStart;
  }

  public Integer getPageSize() {
    return pageSize;
  }

  public void setPageSize(Integer pageSize) {
    this.pageSize = pageSize;
  }

}
