package raven.client.linq;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import raven.abstractions.basic.Lazy;
import raven.abstractions.closure.Function0;
import raven.abstractions.data.FacetAggregation;
import raven.abstractions.data.FacetResult;
import raven.abstractions.data.FacetResults;
import raven.abstractions.extensions.ExpressionExtensions;

import com.mysema.query.types.Path;
import com.mysema.query.types.expr.BooleanExpression;

public class DynamicAggregationQuery<T> {
  private final IRavenQueryable<T> queryable;
  private final List<AggregationQueryDsl> facets;
  private final Map<String, String> renames = new HashMap<String, String>();

  public DynamicAggregationQuery(IRavenQueryable<T> queryable, Path<?> path) {
    this(queryable, path, null);
  }

  public DynamicAggregationQuery(IRavenQueryable<T> queryable, Path<?> path, String displayName) {
    facets = new ArrayList<>();
    this.queryable = queryable;
    andAggregateOn(path, displayName);
  }

  public DynamicAggregationQuery(IRavenQueryable<T> queryable, String path) {
    this(queryable, path, null);
  }

  public DynamicAggregationQuery(IRavenQueryable<T> queryable, String path, String displayName) {
    facets = new ArrayList<>();
    this.queryable = queryable;
    AggregationQueryDsl aggregationQueryDsl = new AggregationQueryDsl();
    aggregationQueryDsl.setName(path);
    aggregationQueryDsl.setDisplayName(displayName);
    facets.add(aggregationQueryDsl);
  }

  public DynamicAggregationQuery<T> andAggregateOn(Path<?> path) {
    return andAggregateOn(path, null);
  }

  public DynamicAggregationQuery<T> andAggregateOn(Path<?> path, String displayName) {
    String propertyPath = ExpressionExtensions.toPropertyPath(path);
    if (isNumeric(path)) {
      String tmp = propertyPath + "_Range";
      renames.put(propertyPath, tmp);
      propertyPath = tmp;
    }

    AggregationQueryDsl aggregationQuery = new AggregationQueryDsl();
    aggregationQuery.setName(propertyPath);
    aggregationQuery.setDisplayName(displayName);
    facets.add(aggregationQuery);
    return this;
  }

  public DynamicAggregationQuery<T> andAggregateOn(String path) {
    return andAggregateOn(path, null);
  }

  public DynamicAggregationQuery<T> andAggregateOn(String path, String displayName) {
    AggregationQueryDsl aggregationQuery = new AggregationQueryDsl();
    aggregationQuery.setName(path);
    aggregationQuery.setDisplayName(displayName);
    facets.add(aggregationQuery);
    return this;
  }

  private boolean isNumeric(Path<?> path) {
    Class< ? > type = path.getType();
    return int.class.equals(type)
        || long.class.equals(type)
        || short.class.equals(type)
        || double.class.equals(type)
        || float.class.equals(type);
  }

  public DynamicAggregationQuery<T> addRanges(BooleanExpression... paths) {
    AggregationQueryDsl last = facets.get(facets.size() - 1);
    if (last.getRanges() == null) {
      last.setRanges(new ArrayList<BooleanExpression>());
    }
    for (BooleanExpression expr: paths) {
      last.getRanges().add(expr);
    }
    return this;
  }

  private void setFacet(Path<?> path, FacetAggregation facetAggregation) {
    AggregationQueryDsl last = facets.get(facets.size() - 1);
    last.setAggregrationField(ExpressionExtensions.toPropertyPath(path));
    last.setAggregationType(path.getType().getName());
    last.getAggregation().add(facetAggregation);
  }

  public DynamicAggregationQuery<T> maxOn(Path<?> path) {
    setFacet(path, FacetAggregation.MAX);
    return this;
  }

  public DynamicAggregationQuery<T> minOn(Path<?> path) {
    setFacet(path, FacetAggregation.MIN);
    return this;
  }

  public DynamicAggregationQuery<T> sumOn(Path<?> path) {
    setFacet(path, FacetAggregation.SUM);
    return this;
  }

  public DynamicAggregationQuery<T> averageOn(Path<?> path) {
    setFacet(path, FacetAggregation.AVERAGE);
    return this;
  }

  public DynamicAggregationQuery<T> countOn(Path<?> path) {
    setFacet(path, FacetAggregation.COUNT);
    return this;
  }

  public FacetResults toList() {
    return handleRenames(queryable.toFacets(AggregationQueryDsl.getFacets(facets)));
  }

  public Lazy<FacetResults> toListLazy() {
    final Lazy<FacetResults> facetsLazy = queryable.toFacetsLazy(AggregationQueryDsl.getFacets(facets));
    return new Lazy<>(new Function0<FacetResults>() {

      @Override
      public FacetResults apply() {
        return handleRenames(facetsLazy.getValue());
      }
    });
  }

  private FacetResults handleRenames(FacetResults facetResults) {
    for (Map.Entry<String, String> rename: renames.entrySet()) {
      if (facetResults.getResults().containsKey(rename.getValue())
          && !facetResults.getResults().containsKey(rename.getKey())) {
        FacetResult value = facetResults.getResults().get(rename.getValue());
        facetResults.getResults().put(rename.getKey(), value);
      }
    }
    return facetResults;
  }

}
