package net.ravendb.client.document;

import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import net.ravendb.abstractions.basic.Lazy;
import net.ravendb.abstractions.basic.Reference;
import net.ravendb.abstractions.basic.SharpEnum;
import net.ravendb.abstractions.basic.Tuple;
import net.ravendb.abstractions.data.Constants;
import net.ravendb.abstractions.data.Facet;
import net.ravendb.abstractions.data.FacetResults;
import net.ravendb.abstractions.data.IndexQuery;
import net.ravendb.abstractions.indexing.SpatialOptions.SpatialRelation;
import net.ravendb.abstractions.indexing.SpatialOptions.SpatialUnits;
import net.ravendb.abstractions.json.linq.RavenJToken;
import net.ravendb.client.FieldHighlightings;
import net.ravendb.client.IDocumentQuery;
import net.ravendb.client.connection.IDatabaseCommands;
import net.ravendb.client.linq.EnumerableUtils;
import net.ravendb.client.listeners.IDocumentQueryListener;
import net.ravendb.client.spatial.SpatialCriteria;

import org.apache.commons.lang.StringUtils;


import com.mysema.query.types.Path;
import com.mysema.query.types.path.ListPath;

/**
 * A query against a Raven index
 * @param <T>
 */
public class DocumentQuery<T> extends AbstractDocumentQuery<T, DocumentQuery<T>> implements IDocumentQuery<T> {

  /**
   * Initializes a new instance of the {@link DocumentQuery} class.
   * @param session
   * @param databaseCommands
   * @param indexName
   * @param fieldsToFetch
   * @param projectionFields
   * @param queryListeners
   * @param isMapReduce
   */
  public DocumentQuery(Class<T> clazz, InMemoryDocumentSessionOperations session , IDatabaseCommands databaseCommands, String indexName,
      String[] fieldsToFetch, String[] projectionFields, List<IDocumentQueryListener> queryListeners, boolean isMapReduce) {
    super(clazz, session, databaseCommands, indexName, fieldsToFetch, projectionFields, queryListeners, isMapReduce);
  }

  /**
   * Initializes a new instance of the {@link DocumentQuery} class.
   * @param other
   */
  public DocumentQuery(DocumentQuery<T> other) {
    super(other);
  }

  /**
   * Selects the projection fields directly from the index
   */
  @Override
  public <TProjection> IDocumentQuery<TProjection> selectFields(Class<TProjection> projectionClass) {
    try {
      List<String> projections = new ArrayList<>();
      List<String> fields = new ArrayList<>();

      Field identityProperty = getDocumentConvention().getIdentityProperty(projectionClass);

      for (PropertyDescriptor propertyDescriptor : Introspector.getBeanInfo(projectionClass).getPropertyDescriptors()) {
        if (propertyDescriptor.getWriteMethod() != null && propertyDescriptor.getReadMethod() != null) {
          projections.add(StringUtils.capitalize(propertyDescriptor.getName()));
          String field = null;
          if (identityProperty != null && propertyDescriptor.getName().equals(identityProperty.getName())) {
            field = Constants.DOCUMENT_ID_FIELD_NAME;
          } else {
            field = propertyDescriptor.getName();
          }
          fields.add(StringUtils.capitalize(field));
        }
      }
      return selectFields(projectionClass, fields.toArray(new String[0]), projections.toArray(new String[0]));

    } catch (IntrospectionException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public IDocumentQuery<T> distinct() {
    distinct = true;
    return this;
  }

  @Override
  public boolean isDistinct() {
    return distinct;
  }

  @Override
  public IDocumentQuery<T> setResultTransformer(String resultsTransformer) {
    this.resultsTransformer = resultsTransformer;
    return this;
  }

  @Override
  public IDocumentQuery<T> orderByScore() {
    addOrder(Constants.TEMPORARY_SCORE_VALUE, false);
    return this;
  }

  @Override
  public IDocumentQuery<T> orderByScoreDescending() {
    addOrder(Constants.TEMPORARY_SCORE_VALUE, true);
    return this;
  }

  @Override
  public IDocumentQuery<T> explainScores() {
    shouldExplainScores = true;
    return this;
  }


  @Override
  public IDocumentQuery<T> sortByDistance() {
    orderBy(Constants.DISTANCE_FIELD_NAME);
    return this;
  }

  @Override
  public void setQueryInputs(Map<String, RavenJToken> queryInputs) {
    this.queryInputs = queryInputs;
  }

  /**
   * Selects the specified fields directly from the index
   * @param projectionClass The class of the projection
   * @param fields The fields.
   * @return
   */
  @Override
  public <TProjection> IDocumentQuery<TProjection> selectFields(Class<TProjection> projectionClass, String... fields) {
    return selectFields(projectionClass, fields, fields);
  }

  /**
   * Selects the specified fields directly from the index
   * @param projectionClass The class of the projection
   * @param fields
   * @param projections
   * @return
   */
  @Override
  public <TProjection> IDocumentQuery<TProjection> selectFields(Class<TProjection> projectionClass, String[] fields, String[] projections) {

    DocumentQuery<TProjection> documentQuery = new DocumentQuery<>(projectionClass, theSession,
        theDatabaseCommands,
        indexName,
        fields,
        projections,
        Arrays.asList(queryListeners),
        isMapReduce);
    documentQuery.pageSize = pageSize;
    documentQuery.queryText = new StringBuilder(queryText.toString());
    documentQuery.start = start;
    documentQuery.timeout = timeout;
    documentQuery.cutoff = cutoff;
    documentQuery.cutoffEtag = cutoffEtag;
    documentQuery.queryStats = queryStats;
    documentQuery.theWaitForNonStaleResults = theWaitForNonStaleResults;
    documentQuery.theWaitForNonStaleResultsAsOfNow = theWaitForNonStaleResultsAsOfNow;
    documentQuery.sortByHints = sortByHints;
    documentQuery.orderByFields = orderByFields;
    documentQuery.distinct = distinct;
    documentQuery.negate = negate;
    documentQuery.includes = new HashSet<>(includes);
    documentQuery.isSpatialQuery = isSpatialQuery;
    documentQuery.spatialFieldName = spatialFieldName;
    documentQuery.queryShape = queryShape;
    documentQuery.spatialRelation = spatialRelation;
    documentQuery.spatialUnits = spatialUnits;
    documentQuery.distanceErrorPct = distanceErrorPct;
    documentQuery.rootTypes.add(clazz);
    documentQuery.defaultField = defaultField;
    documentQuery.beforeQueryExecutionAction = beforeQueryExecutionAction;
    documentQuery.afterQueryExecutedCallback = afterQueryExecutedCallback;
    documentQuery.highlightedFields = new ArrayList<>(highlightedFields);
    documentQuery.highlighterPreTags = highlighterPreTags;
    documentQuery.highlighterPostTags = highlighterPostTags;
    documentQuery.resultsTransformer = resultsTransformer;
    documentQuery.queryInputs = queryInputs;
    documentQuery.disableEntitiesTracking = disableEntitiesTracking;
    documentQuery.disableCaching = disableCaching;
    documentQuery.lastEquality = lastEquality;
    documentQuery.shouldExplainScores = shouldExplainScores;
    return documentQuery;

  }

  @Override
  public IDocumentQuery<T> withinRadiusOf(double radius, double latitude, double longitude) {
    return generateQueryWithinRadiusOf(Constants.DEFAULT_SPATIAL_FIELD_NAME, radius, latitude, longitude);
  }

  @Override
  public IDocumentQuery<T> withinRadiusOf(String fieldName, double radius, double latitude, double longitude) {
    return generateQueryWithinRadiusOf(fieldName, radius, latitude, longitude);
  }

  @Override
  public IDocumentQuery<T> withinRadiusOf(double radius, double latitude, double longitude, SpatialUnits radiusUnits) {
    return generateQueryWithinRadiusOf(Constants.DEFAULT_SPATIAL_FIELD_NAME, radius, latitude, longitude, 0.025, radiusUnits);
  }

  @Override
  public IDocumentQuery<T> withinRadiusOf(String fieldName, double radius, double latitude, double longitude, SpatialUnits radiusUnits) {
    return generateQueryWithinRadiusOf(fieldName, radius, latitude, longitude, 0.025, radiusUnits);
  }

  @Override
  public IDocumentQuery<T> relatesToShape(String fieldName, String shapeWKT, SpatialRelation rel) {
    return generateSpatialQueryData(fieldName, shapeWKT, rel);
  }

  @Override
  public IDocumentQuery<T> relatesToShape(String fieldName, String shapeWKT, SpatialRelation rel, double distanceErroPct) {
    return generateSpatialQueryData(fieldName, shapeWKT, rel, distanceErroPct);
  }

  @Override
  public IDocumentQuery<T> spatial(String fieldName, SpatialCriteria criteria) {
    return generateSpatialQueryData(fieldName, criteria);
  }

  @Override
  public IDocumentQuery<T> not() {
    negateNext();
    return this;
  }

  @Override
  public <TValue> IDocumentQuery<T> whereEquals(Path< ? super TValue > propertySelector, TValue value) {
    whereEquals(getMemberQueryPath(propertySelector), value);
    return this;
  }

  @Override
  public <TValue> IDocumentQuery<T> whereEquals(Path< ? super TValue > propertySelector, TValue value, boolean isAnalyzed) {
    whereEquals(getMemberQueryPath(propertySelector), value, isAnalyzed);
    return this;
  }

  @Override
  public <TValue> IDocumentQuery<T> whereIn(Path< ?  super TValue> propertySelector, Collection<TValue> values) {
    whereIn(getMemberQueryPath(propertySelector), new ArrayList<Object>(values));
    return this;
  }

  @Override
  public <TValue> IDocumentQuery<T> whereStartsWith(Path< ? super TValue > propertySelector, TValue value) {
    whereStartsWith(getMemberQueryPath(propertySelector), value);
    return this;
  }

  @Override
  public <TValue> IDocumentQuery<T> whereEndsWith(Path< ? super TValue > propertySelector, TValue value) {
    whereEndsWith(getMemberQueryPath(propertySelector), value);
    return this;
  }

  @Override
  public <TValue> IDocumentQuery<T> whereBetween(Path< ?  super TValue> propertySelector, TValue start, TValue end) {
    whereBetween(getMemberQueryPath(propertySelector), start, end);
    return this;
  }

  @Override
  public <TValue> IDocumentQuery<T> whereBetweenOrEqual(Path< ? super TValue > propertySelector, TValue start, TValue end) {
    whereBetweenOrEqual(getMemberQueryPath(propertySelector), start, end);
    return this;
  }

  @Override
  public <TValue> IDocumentQuery<T> whereGreaterThan(Path< ? super TValue > propertySelector, TValue value) {
    whereGreaterThan(getMemberQueryPath(propertySelector), value);
    return this;
  }

  @Override
  public <TValue> IDocumentQuery<T> whereGreaterThanOrEqual(Path< ? super TValue > propertySelector, TValue value) {
    whereGreaterThanOrEqual(getMemberQueryPath(propertySelector), value);
    return this;
  }

  @Override
  public <TValue> IDocumentQuery<T> whereLessThan(Path< ?  super TValue> propertySelector, TValue value) {
    whereLessThan(getMemberQueryPath(propertySelector), value);
    return this;
  }

  @Override
  public <TValue> IDocumentQuery<T> whereLessThanOrEqual(Path< ? super TValue > propertySelector, TValue value) {
    whereLessThanOrEqual(getMemberQueryPath(propertySelector), value);
    return this;
  }


  @Override
  public <TValue> IDocumentQuery<T> orderBy(Path< ? >... propertySelectors) {
    String[] orderByfields = getMemberQueryPathsForOrderBy(propertySelectors);
    orderBy(orderByfields);
    for (int index = 0; index < orderByfields.length; index++) {
      String fld = orderByfields[index];
      sortByHints.add(Tuple.<String, Class<?>> create(fld, propertySelectors[index].getType()));
    }
    return this;
  }

  @Override
  public <TValue> IDocumentQuery<T> orderByDescending(Path< ? >... propertySelectors) {
    orderByDescending(getMemberQueryPathsForOrderBy(propertySelectors));
    return this;
  }

  @Override
  public <TValue> IDocumentQuery<T> highlight(Path< ? > propertySelector, int fragmentLength, int fragmentCount, ListPath< ? , ? > fragmentsPropertySelector) {
    String fieldName = getMemberQueryPath(propertySelector);
    String fragmentsField = getMemberQueryPath(fragmentsPropertySelector);
    highlight(fieldName, fragmentLength, fragmentCount, fragmentsField);
    return this;
  }

  @Override
  public <TValue> IDocumentQuery<T> highlight(Path< ? > propertySelector, int fragmentLength, int fragmentCount, Reference<FieldHighlightings> highlightings) {
    highlight(getMemberQueryPath(propertySelector), fragmentLength, fragmentCount, highlightings);
    return this;
  }

  @Override
  public <TValue> IDocumentQuery<T> addOrder(Path< ? > propertySelector, boolean descending) {
    addOrder(getMemberQueryPath(propertySelector), descending);
    return this;
  }

  @Override
  public <TValue> IDocumentQuery<T> search(Path< ? > propertySelector, String searchTerms) {
    search(getMemberQueryPath(propertySelector), searchTerms);
    return this;
  }

  /**
   * Performs a query matching ANY of the provided values against the given field (OR)
   */
  @Override
  public IDocumentQuery<T> containsAny(Path< ? > propertySelector, Collection<Object> values) {
    containsAny(getMemberQueryPath(propertySelector), values);
    return this;
  }

  /**
   * Performs a query matching ALL of the provided values against the given field (AND)
   */
  @Override
  public IDocumentQuery<T> containsAll(Path< ? > propertySelector, Collection<Object> values) {
    containsAll(getMemberQueryPath(propertySelector), values);
    return this;
  }

  @Override
  public IDocumentQuery<T> spatial(Path< ? > path, SpatialCriteria criteria) {
    return spatial(getMemberQueryPath(path), criteria);
  }

  @Override
  public String toString() {
    String query = super.toString();
    if (isSpatialQuery) {
      return String.format("%s SpatialField: %s QueryShape: %s Relation: %s", query, spatialFieldName, queryShape, SharpEnum.value(spatialRelation));
    }
    return query;
  }

  @Override
  public Lazy<FacetResults> toFacetsLazy(String facetSetupDoc) {
    return toFacetsLazy(facetSetupDoc, 0, null);
  }

  @Override
  public Lazy<FacetResults> toFacetsLazy(String facetSetupDoc, int start) {
    return toFacetsLazy(facetSetupDoc, start, null);
  }

  @Override
  public Lazy<FacetResults> toFacetsLazy(String facetSetupDoc, int start, Integer pageSize) {
    IndexQuery indexQuery = getIndexQuery();

    LazyFacetsOperation lazyOperation = new LazyFacetsOperation(getIndexQueried(), facetSetupDoc, indexQuery, start, pageSize);
    DocumentSession documentSession = (DocumentSession) getSession();
    return documentSession.addLazyOperation(lazyOperation, null);
  }

  @Override
  public Lazy<FacetResults> toFacetsLazy(List<Facet> facets) {
    return toFacetsLazy(facets, 0, null);
  }

  @Override
  public Lazy<FacetResults> toFacetsLazy(List<Facet> facets, int start) {
    return toFacetsLazy(facets, start, null);
  }

  @Override
  public Lazy<FacetResults> toFacetsLazy(List<Facet> facets, int start, Integer pageSize) {
    if (facets.isEmpty()) {
      throw new IllegalArgumentException("Facets must contain at least one entry");
    }
    IndexQuery indexQuery = getIndexQuery();
    LazyFacetsOperation lazyOperation = new LazyFacetsOperation(getIndexQueried(), facets, indexQuery, start, pageSize);
    DocumentSession documentSession = (DocumentSession) getSession();
    return documentSession.addLazyOperation(lazyOperation, null);
  }

  @Override
  public FacetResults toFacets(String facetSetupDoc) {
    return toFacets(facetSetupDoc, 0, null);
  }

  @Override
  public FacetResults toFacets(String facetSetupDoc, int start) {
    return toFacets(facetSetupDoc, start, null);
  }

  @Override
  public FacetResults toFacets(String facetSetupDoc, int start, Integer pageSize) {
    return getFacets(facetSetupDoc, start, pageSize);
  }

  @Override
  public FacetResults toFacets(List<Facet> facets) {
    return toFacets(facets, 0, null);
  }

  @Override
  public FacetResults toFacets(List<Facet> facets, int start) {
    return toFacets(facets, start, null);
  }

  @Override
  public FacetResults toFacets(List<Facet> facets, int start, Integer pageSize) {
    if (facets.isEmpty()) {
      throw new IllegalArgumentException("Facets must contain at least one entry");
    }
    return getFacets(facets, start, pageSize);
  }

  @Override
  public List<T> toList() {
    return EnumerableUtils.toList(iterator());
  }

  @Override
  public T single() {
    return EnumerableUtils.single(iterator());
  }

  @Override
  public T first() {
    return EnumerableUtils.first(iterator());
  }

  @Override
  public boolean any() {
    return EnumerableUtils.any(iterator());
  }

  @Override
  public T firstOrDefault() {
    return EnumerableUtils.firstOrDefault(iterator());
  }

  @Override
  public T singleOrDefault() {
    return EnumerableUtils.singleOrDefault(iterator());
  }


}
