package net.ravendb.abstractions.data;

import java.util.ArrayList;
import java.util.List;

public class FacetSetup {
  private String id;
  private List<Facet> facets = new ArrayList<>();


  public FacetSetup() {
    super();
  }
  public FacetSetup(String id, List<Facet> facets) {
    super();
    this.id = id;
    this.facets = facets;
  }
  public String getId() {
    return id;
  }
  public void setId(String id) {
    this.id = id;
  }
  public List<Facet> getFacets() {
    return facets;
  }
  public void setFacets(List<Facet> facets) {
    this.facets = facets;
  }


}
