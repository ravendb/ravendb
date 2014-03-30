package net.ravendb.client.linq;

import com.mysema.query.types.expr.BooleanExpression;

import java.util.ArrayList;
import java.util.List;

import net.ravendb.abstractions.data.Facet;
import net.ravendb.abstractions.data.FacetAggregationSet;
import net.ravendb.abstractions.data.FacetMode;


public class AggregationQueryDsl extends AggregationQuery {
  private List<BooleanExpression> ranges = new ArrayList<>();

  public List<BooleanExpression> getRanges() {
    return ranges;
  }

  public void setRanges(List<BooleanExpression> ranges) {
    this.ranges = ranges;
  }

  public static List<Facet> getDslFacets(List<AggregationQueryDsl> aggregationQueries) {
    List<Facet> facetsList = new ArrayList<>();
    for (AggregationQueryDsl aggregationQuery : aggregationQueries) {
      if (aggregationQuery.getAggregation().equals(new FacetAggregationSet())) {
        throw new IllegalStateException("All aggregations must have a type");
      }

      boolean shouldUseRanges = aggregationQuery.getRanges() != null && aggregationQuery.getRanges().size() > 0;
      List<String> ranges =  new ArrayList<>();
      if (shouldUseRanges) {
        for (BooleanExpression expr: aggregationQuery.getRanges()) {
          ranges.add(Facet.parse(expr));
        }
      }

      FacetMode mode = shouldUseRanges ? FacetMode.RANGES : FacetMode.DEFAULT;

      Facet facet = new Facet();
      facet.setName(aggregationQuery.getName());
      facet.setDisplayName(aggregationQuery.getDisplayName() != null ? aggregationQuery.getDisplayName()  : aggregationQuery.getName());
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
