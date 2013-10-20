package net.ravendb.client.linq;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.ravendb.abstractions.basic.Lazy;
import net.ravendb.abstractions.closure.Function0;
import net.ravendb.abstractions.data.FacetAggregation;
import net.ravendb.abstractions.data.FacetResult;
import net.ravendb.abstractions.data.FacetResults;
import net.ravendb.abstractions.extensions.ExpressionExtensions;


import com.mysema.query.types.Path;
import com.mysema.query.types.expr.BooleanExpression;

public class DynamicAggregationQuery<T> {
  private final IRavenQueryable<T> queryable;
  private final List<AggregationQueryDsl> facets;
  private final Map<String, String> renames = new HashMap<>();

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
    return int.class.equals(type) || Integer.class.equals(type)
        || long.class.equals(type) || Long.class.equals(type)
        || short.class.equals(type) || Short.class.equals(type)
        || double.class.equals(type) || Double.class.equals(type)
        || float.class.equals(type) || Float.class.equals(type);
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
    last.setAggregationType(mapJavaToDotNetNumberClass(path.getType()));
    last.getAggregation().add(facetAggregation);
  }

  private String mapJavaToDotNetNumberClass(Class<?> clazz) {
    if (Integer.class.equals(clazz)) {
      return "System.Int32";
    } else if (Long.class.equals(clazz)){
      return "System.Int64";
    } else if (Float.class.equals(clazz)) {
      return "System.Single";
    } else if (Double.class.equals(clazz)) {
      return "System.Double";
    } else if (Short.class.equals(clazz)) {
      return "System.Int16";
    } else {
      return clazz.getCanonicalName();
    }
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
    return handleRenames(queryable.toFacets(AggregationQueryDsl.getDslFacets(facets)));
  }

  public Lazy<FacetResults> toListLazy() {
    final Lazy<FacetResults> facetsLazy = queryable.toFacetsLazy(AggregationQueryDsl.getDslFacets(facets));
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
