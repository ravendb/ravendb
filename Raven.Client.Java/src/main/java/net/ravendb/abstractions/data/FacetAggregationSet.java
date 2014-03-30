package net.ravendb.abstractions.data;

import java.util.Arrays;

import org.codehaus.jackson.annotate.JsonCreator;

public class FacetAggregationSet extends EnumSet<FacetAggregation, FacetAggregationSet> {

  public FacetAggregationSet() {
    super(FacetAggregation.class);
  }

  public FacetAggregationSet(FacetAggregation...values) {
    super(FacetAggregation.class, Arrays.asList(values));
  }

  public static FacetAggregationSet of(FacetAggregation... values) {
    return new FacetAggregationSet(values);
  }

  @JsonCreator
  static FacetAggregationSet construct(int value) {
    return construct(new FacetAggregationSet(), value);
  }

  @JsonCreator
  static FacetAggregationSet construct(String value) {
    return construct(new FacetAggregationSet(), value);
  }

}
