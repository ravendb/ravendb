package raven.client.document;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import raven.abstractions.basic.Reference;
import raven.abstractions.basic.Tuple;
import raven.abstractions.closure.Function1;
import raven.abstractions.closure.Function2;
import raven.abstractions.data.AggregationOperation;
import raven.abstractions.data.Constants;
import raven.abstractions.data.IndexQuery;
import raven.abstractions.indexing.SpatialOptions.SpatialRelation;
import raven.abstractions.indexing.SpatialOptions.SpatialUnits;
import raven.abstractions.json.linq.RavenJToken;
import raven.client.FieldHighlightings;
import raven.client.IDocumentQuery;
import raven.client.connection.IDatabaseCommands;
import raven.client.listeners.IDocumentQueryListener;
import raven.client.spatial.SpatialCriteria;
import raven.client.spatial.SpatialCriteriaFactory;

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
    /*TODO:
     * var propertyInfos = typeof (TProjection).GetProperties(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance);
      var projections = propertyInfos.Select(x => x.Name).ToArray();
      var identityProperty = DocumentConvention.GetIdentityProperty(typeof (TProjection));
      var fields = propertyInfos.Select(p =>(p == identityProperty) ? Constants.DocumentIdFieldName : p.Name).ToArray();
      return SelectFields<TProjection>(fields, projections);
     */
    return null; //TODO delete me
  }

  public IDocumentQuery<T> setResultTransformer(String resultsTransformer) {
    this.resultsTransformer = resultsTransformer;
    return this;
  }

  public IDocumentQuery<T> orderByScore() {
    addOrder(Constants.TEMPORARY_SCORE_VALUE, false);
    return this;
  }

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
    return (IDocumentQuery<T>) this;
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
  public <TProjection> IDocumentQuery<TProjection> selectFields(Class<TProjection> projectionClass, String[] fields, String[] projections) {

    DocumentQuery<TProjection> documentQuery = new DocumentQuery<TProjection>(projectionClass, theSession,
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
    documentQuery.sortByHints = sortByHints;
    documentQuery.orderByFields = orderByFields;
    documentQuery.groupByFields = groupByFields;
    documentQuery.aggregationOp = aggregationOp;
    documentQuery.negate = negate;
    documentQuery.transformResultsFunc = transformResultsFunc;
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
  public IDocumentQuery<T> spatial(String fieldName, Function1<SpatialCriteriaFactory, SpatialCriteria> clause) {
    SpatialCriteria criteria = clause.apply(new SpatialCriteriaFactory());
    return generateSpatialQueryData(fieldName, criteria);
  }

  public IDocumentQuery<T> transformResults(Function2<IndexQuery, Collection<Object>, Collection<Object>> resultsTransformer) {
    this.transformResultsFunc = resultsTransformer;
    return (IDocumentQuery<T>) this;
  }

  @Override
  public IDocumentQuery<T> not() {
    negateNext();
    return null;
  }

  @Override
  public <TValue> IDocumentQuery<T> whereEquals(Path< ? > propertySelector, TValue value) {
    whereEquals(getMemberQueryPath(propertySelector), value);
    return this;
  }

  @Override
  public <TValue> IDocumentQuery<T> whereEquals(Path< ? > propertySelector, TValue value, boolean isAnalyzed) {
    whereEquals(getMemberQueryPath(propertySelector), value, isAnalyzed);
    return this;
  }

  @Override
  public <TValue> IDocumentQuery<T> whereIn(Path< ? > propertySelector, Collection<TValue> values) {
    whereIn(getMemberQueryPath(propertySelector), new ArrayList<Object>(values));
    return this;
  }

  @Override
  public <TValue> IDocumentQuery<T> whereStartsWith(Path< ? > propertySelector, TValue value) {
    whereStartsWith(getMemberQueryPath(propertySelector), value);
    return this;
  }

  @Override
  public <TValue> IDocumentQuery<T> whereEndsWith(Path< ? > propertySelector, TValue value) {
    whereEndsWith(getMemberQueryPath(propertySelector), value);
    return this;
  }

  @Override
  public <TValue> IDocumentQuery<T> whereBetween(Path< ? > propertySelector, TValue start, TValue end) {
    whereBetween(getMemberQueryPath(propertySelector), start, end);
    return this;
  }

  @Override
  public <TValue> IDocumentQuery<T> whereBetweenOrEqual(Path< ? > propertySelector, TValue start, TValue end) {
    whereBetweenOrEqual(getMemberQueryPath(propertySelector), start, end);
    return this;
  }

  @Override
  public <TValue> IDocumentQuery<T> whereGreaterThan(Path< ? > propertySelector, TValue value) {
    whereGreaterThan(getMemberQueryPath(propertySelector), value);
    return this;
  }

  @Override
  public <TValue> IDocumentQuery<T> whereGreaterThanOrEqual(Path< ? > propertySelector, TValue value) {
    whereGreaterThanOrEqual(getMemberQueryPath(propertySelector), value);
    return this;
  }

  @Override
  public <TValue> IDocumentQuery<T> whereLessThan(Path< ? > propertySelector, TValue value) {
    whereLessThan(getMemberQueryPath(propertySelector), value);
    return this;
  }

  @Override
  public <TValue> IDocumentQuery<T> whereLessThanOrEqual(Path< ? > propertySelector, TValue value) {
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

  @Override
  public <TValue> IDocumentQuery<T> groupBy(EnumSet<AggregationOperation> aggregationOperation, Path< ? >... groupPropertySelectors) {
    groupBy(aggregationOperation, getMemberQueryPaths(groupPropertySelectors));
    return this;
  }

  @Override
  public IDocumentQuery<T> spatial(Path< ? > path, Function1<SpatialCriteriaFactory, SpatialCriteria> clause) {
    return spatial(getMemberQueryPath(path), clause);
  }

  @Override
  public IDocumentQuery<T> whereIn(String fieldName, Collection<Object> values) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public T first() {
    pageSize = 1;
    List<T> list = toList();
    if (list.isEmpty()) {
      return null;
    } else {
      return list.get(0);
    }
  }

  @Override
  public List<T> toList() {

    initSync();
    while (true) {
      try {
        return queryOperation.complete(clazz);
      } catch (Exception e) {
        if (!queryOperation.shouldQueryAgain(e)) {
          throw e;
        }
        executeActualQuery(); // retry the query, note that we explicitly not incrementing the session request count here
      }
    }
  }

  public String toString() {
    String query = super.toString();
    if (isSpatialQuery) {
      return String.format("%s SpatialField: %s QueryShape: %s Relation: %s", query, spatialFieldName, queryShape, spatialRelation);
    }
    return query;
  }

}
