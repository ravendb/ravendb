package net.ravendb.client.linq;

import java.util.ArrayList;
import java.util.List;

import net.ravendb.abstractions.data.Facet;
import net.ravendb.abstractions.data.FacetAggregationSet;
import net.ravendb.abstractions.data.FacetMode;


public class AggregationQuery {

  private String name;
  private String displayName;
  private String aggregrationField;
  private String aggregationType;
  private FacetAggregationSet aggregation = new FacetAggregationSet();

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getDisplayName() {
    return displayName;
  }

  public void setDisplayName(String displayName) {
    this.displayName = displayName;
  }

  public String getAggregrationField() {
    return aggregrationField;
  }

  public void setAggregrationField(String aggregrationField) {
    this.aggregrationField = aggregrationField;
  }

  public String getAggregationType() {
    return aggregationType;
  }

  public void setAggregationType(String aggregationType) {
    this.aggregationType = aggregationType;
  }

  public FacetAggregationSet getAggregation() {
    return aggregation;
  }

  public void setAggregation(FacetAggregationSet aggregation) {
    this.aggregation = aggregation;
  }

  public static List<Facet> getFacets(List<AggregationQuery> aggregationQueries) {
    List<Facet> facetsList = new ArrayList<>();
    for (AggregationQuery aggregationQuery : aggregationQueries) {
      if (aggregationQuery.getAggregation().equals(new FacetAggregationSet())) {
        throw new IllegalStateException("All aggregations must have a type");
      }

      Facet facet = new Facet();
      facet.setName(aggregationQuery.getName());
      facet.setDisplayName(aggregationQuery.getDisplayName());
      facet.setAggregation(aggregationQuery.getAggregation());
      facet.setAggregationType(aggregationQuery.getAggregationType());
      facet.setAggregationField(aggregationQuery.getAggregrationField());
      facet.setMode(FacetMode.DEFAULT);

      facetsList.add(facet);
    }
    return facetsList;
  }

}
