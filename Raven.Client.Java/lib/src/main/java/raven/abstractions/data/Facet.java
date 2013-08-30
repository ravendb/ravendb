package raven.abstractions.data;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

public class Facet {
  private String displayName;
  private FacetMode mode = FacetMode.DEFAULT;
  private EnumSet<FacetAggregation> aggregation = EnumSet.of(FacetAggregation.NONE);
  private String aggregationField;
  private String aggregationType;
  private String name;
  private List<String> ranges;
  private Integer maxResults;
  private FacetTermSortMode termSortMode;
  private boolean includeRemainingTerms;

  public Facet() {
    this.ranges = new ArrayList<>();
    this.termSortMode = FacetTermSortMode.VALUE_ASC;
  }

  public EnumSet<FacetAggregation> getAggregation() {
    return aggregation;
  }

  public void setAggregation(EnumSet<FacetAggregation> aggregation) {
    this.aggregation = aggregation;
  }



  public String getAggregationType() {
    return aggregationType;
  }

  public void setAggregationType(String aggregationType) {
    this.aggregationType = aggregationType;
  }

  public String getAggregationField() {
    return aggregationField;
  }
  public String getDisplayName() {
    return displayName;
  }
  public Integer getMaxResults() {
    return maxResults;
  }
  public FacetMode getMode() {
    return mode;
  }
  public String getName() {
    return name;
  }
  public List<String> getRanges() {
    return ranges;
  }
  public FacetTermSortMode getTermSortMode() {
    return termSortMode;
  }
  public boolean isIncludeRemainingTerms() {
    return includeRemainingTerms;
  }
  public void setAggregationField(String aggregationField) {
    this.aggregationField = aggregationField;
  }
  public void setDisplayName(String displayName) {
    this.displayName = displayName;
  }
  public void setIncludeRemainingTerms(boolean includeRemainingTerms) {
    this.includeRemainingTerms = includeRemainingTerms;
  }
  public void setMaxResults(Integer maxResults) {
    this.maxResults = maxResults;
  }
  public void setMode(FacetMode mode) {
    this.mode = mode;
  }
  public void setName(String name) {
    this.name = name;
  }
  public void setRanges(List<String> ranges) {
    this.ranges = ranges;
  }
  public void setTermSortMode(FacetTermSortMode termSortMode) {
    this.termSortMode = termSortMode;
  }

}
