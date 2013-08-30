package raven.client.linq;

import com.mysema.query.types.expr.BooleanExpression;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

import raven.abstractions.data.Facet;
import raven.abstractions.data.FacetAggregation;
import raven.abstractions.data.FacetDsl;
import raven.abstractions.data.FacetMode;

public class AggregationQueryDsl extends AggregationQuery {
  private List<BooleanExpression> ranges;

  public List<BooleanExpression> getRanges() {
    return ranges;
  }

  public void setRanges(List<BooleanExpression> ranges) {
    this.ranges = ranges;
  }

  public static List<Facet> getFacets(List<AggregationQueryDsl> aggregationQueries) {
    List<Facet> facetsList = new ArrayList<>();
    for (AggregationQueryDsl aggregationQuery : aggregationQueries) {
      if (aggregationQuery.getAggregation().equals(EnumSet.of(FacetAggregation.NONE))) {
        throw new IllegalStateException("All aggregations must have a type");
      }

      boolean shouldUseRanges = aggregationQuery.getRanges() != null && aggregationQuery.getRanges().size() > 0;
      List<String> ranges =  new ArrayList<>();
      if (shouldUseRanges) {
        for (BooleanExpression expr: aggregationQuery.getRanges()) {
          ranges.add(FacetDsl.parse(expr));
        }
      }

      FacetMode mode = shouldUseRanges ? FacetMode.RANGES : FacetMode.DEFAULT;

      Facet facet = new Facet();
      facet.setName(aggregationQuery.getName());
      facet.setDisplayName(aggregationQuery.getDisplayName());
      facet.setAggregation(aggregationQuery.getAggregation());
      facet.setAggregationType(aggregationQuery.getAggregationType());
      facet.setAggregationField(aggregationQuery.getAggregrationField());
      facet.setRanges(ranges);
      facet.setMode(mode);

      facetsList.add(facet);
    }
    return facetsList;
  }

}
