package raven.client.document;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;

import com.mysema.query.dml.UpdateClause;
import com.mysema.query.types.ExpressionUtils;
import com.mysema.query.types.Path;

import raven.abstractions.basic.Lazy;
import raven.abstractions.basic.Reference;
import raven.abstractions.basic.Tuple;
import raven.abstractions.closure.Action1;
import raven.abstractions.closure.Action2;
import raven.abstractions.closure.Delegates;
import raven.abstractions.closure.Function1;
import raven.abstractions.closure.Function2;
import raven.abstractions.data.AggregationOperation;
import raven.abstractions.data.Constants;
import raven.abstractions.data.Etag;
import raven.abstractions.data.FacetResults;
import raven.abstractions.data.HighlightedField;
import raven.abstractions.data.IndexQuery;
import raven.abstractions.data.QueryOperator;
import raven.abstractions.data.QueryResult;
import raven.abstractions.data.SpatialIndexQuery;
import raven.abstractions.extensions.ExpressionExtensions;
import raven.abstractions.indexing.SpatialOptions.SpatialRelation;
import raven.abstractions.indexing.SpatialOptions.SpatialUnits;
import raven.abstractions.json.linq.RavenJToken;
import raven.abstractions.spatial.WktSanitizer;
import raven.client.FieldHighlightings;
import raven.client.IDocumentQuery;
import raven.client.IDocumentQueryCustomization;
import raven.client.IDocumentSession;
import raven.client.RavenQueryHighlightings;
import raven.client.RavenQueryStatistics;
import raven.client.WhereParams;
import raven.client.connection.IDatabaseCommands;
import raven.client.connection.IRavenQueryInspector;
import raven.client.document.sessionoperations.QueryOperation;
import raven.client.linq.LinqPathProvider;
import raven.client.listeners.IDocumentQueryListener;
import raven.client.spatial.SpatialCriteria;
import raven.client.spatial.SpatialCriteriaFactory;

/**
 * A query against a Raven index
 *
 */
public abstract class AbstractDocumentQuery<T, TSelf extends AbstractDocumentQuery<T, TSelf>> implements IDocumentQueryCustomization, IRavenQueryInspector, IAbstractDocumentQuery<T> {
  protected boolean isSpatialQuery;
  protected String spatialFieldName, queryShape;
  protected SpatialUnits spatialUnits;
  protected SpatialRelation spatialRelation;
  protected double distanceErrorPct;
  private final LinqPathProvider linqPathProvider;
  protected Action1<IndexQuery> beforeQueryExecutionAction;

  protected final Set<Class<T>> rootTypes = new HashSet<>(); //TODO: add typeof(T)

  static Map<Class< ? >, Function1<Object, String>> implicitStringsCache = new HashMap<Class< ? >, Function1<Object, String>>();

  /**
   * Whatever to negate the next operation
   */
  protected boolean negate;

  /**
   * The database commands to use
   */
  protected final IDatabaseCommands theDatabaseCommands;

  /**
   * The index to query
   */
  protected final String indexName;

  protected Function2<IndexQuery, Collection<Object>, Collection<Object>> transformResultsFunc;

  protected String defaultField;

  private int currentClauseDepth;

  protected Tuple<String, String> lastEquality;

  protected Map<String, RavenJToken> queryInputs = new HashMap<>();

  /**
   * The list of fields to project directly from the results
   */
  protected final String[] projectionFields;

  /**
   * The list of fields to project directly from the index on the server
   */
  protected final String[] fieldsToFetch;

  /**
   * The query listeners for this query
   */
  protected final IDocumentQueryListener[] queryListeners;
  protected final boolean isMapReduce;

  /**
   * The session for this query
   */
  protected final InMemoryDocumentSessionOperations theSession;

  /**
   * The cutoff date to use for detecting staleness in the index
   */
  protected Date cutoff;

  /**
   * The fields to order the results by
   */
  protected String[] orderByFields = new String[0];

  /**
   * The fields to highlight
   */
  protected List<HighlightedField> highlightedFields = new ArrayList<HighlightedField>();

  /**
   * Highlighter pre tags
   */
  protected String[] highlighterPreTags = new String[0];

  /**
   * Highlighter post tags
   */
  protected String[] highlighterPostTags = new String[0];

  /**
   * The types to sort the fields by (NULL if not specified)
   */
  protected Set<Tuple<String, Class< ? >>> sortByHints = new HashSet<>();

  /**
   * The page size to use when querying the index
   */
  protected Integer pageSize;

  protected QueryOperation queryOperation;

  /**
   * The query to use
   */
  protected StringBuilder queryText = new StringBuilder();

  /**
   * which record to start reading from
   */
  protected int start;

  private DocumentConvention conventions;
  /**
   * Timeout for this query
   */
  protected long timeout;
  /**
   * Should we wait for non stale results
   */
  protected boolean theWaitForNonStaleResults;
  /**
   * The paths to include when loading the query
   */
  protected Set<String> includes = new HashSet<>();
  /**
   * What aggregated operation to execute
   */
  protected AggregationOperation aggregationOp;

  /**
   * Fields to group on
   */
  protected String[] groupByFields;

  /**
   * Holds the query stats
   */
  protected RavenQueryStatistics queryStats = new RavenQueryStatistics();

  /**
   * Holds the query highlightings
   */
  protected RavenQueryHighlightings highlightings = new RavenQueryHighlightings();

  /**
   * The name of the results transformer to use after executing this query
   */
  protected String resultsTransformer;

  /**
   * Determines if entities should be tracked and kept in memory
   */
  protected boolean disableEntitiesTracking;

  /*
   * Determine if query results should be cached.
   */
  protected boolean disableCaching;

  /**
   * Determine if scores of query results should be explained
   */
  protected boolean shouldExplainScores;

  protected Action1<QueryResult> afterQueryExecutedCallback;
  protected Etag cutoffEtag;

  /**
   * Get the name of the index being queried
   */
  public String getIndexQueried() {
    return indexName;
  }

  /**
   * Grant access to the database commands
   */
  public IDatabaseCommands getDatabaseCommands() {
    return theDatabaseCommands;
  }

  /**
   * Gets the document convention from the query session
   * @return
   */
  public DocumentConvention getDocumentConvention() {
    return conventions;
  }

  /**
   * Gets the session associated with this document query
   */
  public InMemoryDocumentSessionOperations getSession() {
    return (InMemoryDocumentSessionOperations) theSession;
  }

  private long getDefaultTimeout() {
    return 15 * 1000;
  }

  public AbstractDocumentQuery(InMemoryDocumentSessionOperations theSession, IDatabaseCommands databaseCommands, String indexName, String[] fieldsToFetch, String[] projectionFields,
      IDocumentQueryListener[] queryListeners, boolean isMapReduce) {
    this.theDatabaseCommands = databaseCommands;
    this.projectionFields = projectionFields;
    this.fieldsToFetch = fieldsToFetch;
    this.queryListeners = queryListeners;
    this.isMapReduce = isMapReduce;
    this.indexName = indexName;
    this.theSession = theSession;
    this.afterQueryExecutedCallback = new Action1<QueryResult>() {
      @Override
      public void apply(QueryResult result) {
        updateStatsAndHighlightings(result);
      }
    };

    conventions = theSession == null ? new DocumentConvention() : theSession.getConventions();
    linqPathProvider = new LinqPathProvider(conventions);

    if (conventions.getDefaultQueryingConsistency() == ConsistencyOptions.QUERY_YOUR_WRITES) {
      waitForNonStaleResultsAsOfLastWrite();
    }
  }

  private void updateStatsAndHighlightings(QueryResult queryResult) {
    this.queryStats.updateQueryStats(queryResult);
    this.highlightings.update(queryResult);
  }

  protected AbstractDocumentQuery(AbstractDocumentQuery<T, TSelf> other) {
    theDatabaseCommands = other.theDatabaseCommands;
    indexName = other.indexName;
    linqPathProvider = other.linqPathProvider;
    projectionFields = other.projectionFields;
    theSession = other.theSession;
    conventions = other.conventions;
    cutoff = other.cutoff;
    orderByFields = other.orderByFields;
    sortByHints = other.sortByHints;
    pageSize = other.pageSize;
    queryText = other.queryText;
    start = other.start;
    timeout = other.timeout;
    theWaitForNonStaleResults = other.theWaitForNonStaleResults;
    includes = other.includes;
    queryListeners = other.queryListeners;
    queryStats = other.queryStats;
    defaultOperator = other.defaultOperator;
    defaultField = other.defaultField;
    highlightedFields = other.highlightedFields;
    highlighterPreTags = other.highlighterPreTags;
    highlighterPostTags = other.highlighterPostTags;
    queryInputs = other.queryInputs;
    disableEntitiesTracking = other.disableEntitiesTracking;
    disableCaching = other.disableCaching;
    shouldExplainScores = other.shouldExplainScores;

    afterQueryExecuted(new Action1<QueryResult>() {
      @Override
      public void apply(QueryResult result) {
        updateStatsAndHighlightings(result);
      }
    });
  }

  @Override
  public IDocumentQueryCustomization include(String path) {
    includes.add(path);
    return this;
  }

  @Override
  public IDocumentQueryCustomization waitForNonStaleResults(long waitTimeout) {
    theWaitForNonStaleResults = true;
    cutoffEtag = null;
    cutoff = null;
    timeout = waitTimeout;
    return this;
  }

  @Override
  public IDocumentQueryCustomization sortByDistance() {
    orderBy(Constants.DISTANCE_FIELD_NAME);
    return this;
  }

  @Override
  public IDocumentQueryCustomization withinRadiusOf(double radius, double latitude, double longitude) {
    generateQueryWithinRadiusOf(Constants.DEFAULT_SPATIAL_FIELD_NAME, radius, latitude, longitude);
    return this;
  }

  @Override
  public IDocumentQueryCustomization withinRadiusOf(String fieldName, double radius, double latitude, double longitude) {
    generateQueryWithinRadiusOf(fieldName, radius, latitude, longitude);
    return this;
  }

  @Override
  public IDocumentQueryCustomization withinRadiusOf(double radius, double latitude, double longitude, SpatialUnits radiusUnits) {
    generateQueryWithinRadiusOf(Constants.DEFAULT_SPATIAL_FIELD_NAME, radius, latitude, longitude, 0.025, radiusUnits);
    return this;
  }

  @Override
  public IDocumentQueryCustomization withinRadiusOf(String fieldName, double radius, double latitude, double longitude, SpatialUnits radiusUnits) {
    generateQueryWithinRadiusOf(fieldName, radius, latitude, longitude, 0.025, radiusUnits);
    return this;
  }

  @Override
  public IDocumentQueryCustomization relatesToShape(String fieldName, String shapeWKT, SpatialRelation rel) {
    generateSpatialQueryData(fieldName, shapeWKT, rel);
    return this;
  }

  @Override
  public IDocumentQueryCustomization spatial(String fieldName, Function1<SpatialCriteriaFactory, SpatialCriteria> clause) {
    SpatialCriteria criteria = clause.apply(new SpatialCriteriaFactory());
    generateSpatialQueryData(fieldName, criteria);
    return this;
  }

  protected TSelf generateQueryWithinRadiusOf(String fieldName, double radius, double latitude, double longitude) {
    generateQueryWithinRadiusOf(fieldName, radius, latitude, longitude, 0.025, (SpatialUnits) null);
    return (TSelf)this;
  }


  protected TSelf generateQueryWithinRadiusOf(String fieldName, double radius, double latitude, double longitude, double distanceErrorPct) {
    generateQueryWithinRadiusOf(fieldName, radius, latitude, longitude, distanceErrorPct,(SpatialUnits) null);
    return (TSelf)this;
  }
  /**
   * Filter matches to be inside the specified radius
   * @param fieldName
   * @param radius
   * @param latitude
   * @param longitude
   * @param distanceErrorPct
   * @return
   */
  protected TSelf generateQueryWithinRadiusOf(String fieldName, double radius, double latitude, double longitude, double distanceErrorPct, SpatialUnits radiusUnits) {
    return generateSpatialQueryData(fieldName, SpatialIndexQuery.getQueryShapeFromLatLon(latitude, longitude, radius), SpatialRelation.WITHIN, distanceErrorPct, radiusUnits);
  }

  protected TSelf generateSpatialQueryData(String fieldName, String shapeWKT, SpatialRelation relation) {
    generateSpatialQueryData(fieldName, shapeWKT, relation, 0.025, null);
    return (TSelf) this;
  }

  protected TSelf generateSpatialQueryData(String fieldName, String shapeWKT, SpatialRelation relation, double distanceErrorPct) {
    generateSpatialQueryData(fieldName, shapeWKT, relation, distanceErrorPct, null);
    return (TSelf) this;
  }

  protected TSelf generateSpatialQueryData(String fieldName, String shapeWKT, SpatialRelation relation, double distanceErrorPct, SpatialUnits radiusUnits) {
    isSpatialQuery = true;
    spatialFieldName = fieldName;
    queryShape = new WktSanitizer().sanitize(shapeWKT);
    spatialRelation = relation;
    this.distanceErrorPct = distanceErrorPct;
    spatialUnits = radiusUnits;
    return (TSelf) this;
  }

  protected TSelf generateSpatialQueryData(String fieldName, SpatialCriteria criteria) {
    generateSpatialQueryData(fieldName, criteria, 0.025);
    return (TSelf) this;
  }

  protected TSelf generateSpatialQueryData(String fieldName, SpatialCriteria criteria, double distanceErrorPct) {
    /*TODO:
    var wkt = criteria.Shape as string;
    if (wkt == null && criteria.Shape != null)
    {
      var jsonSerializer = DocumentConvention.CreateSerializer();

      using (var jsonWriter = new RavenJTokenWriter())
      {
        var converter = new ShapeConverter();
        jsonSerializer.Serialize(jsonWriter, criteria.Shape);
        if (!converter.TryConvert(jsonWriter.Token, out wkt))
          throw new ArgumentException("Shape");
      }
    }

    if (wkt == null)
      throw new ArgumentException("Shape");

    isSpatialQuery = true;
    spatialFieldName = fieldName;
    queryShape = new WktSanitizer().Sanitize(wkt);
    spatialRelation = criteria.Relation;
    this.distanceErrorPct = distanceErrorPct; */
    return (TSelf) this;
  }

  @Override
  public IDocumentQueryCustomization waitForNonStaleResults() {
    waitForNonStaleResults(getDefaultTimeout());
    return this;
  }

  public void usingDefaultField(String field) {
    defaultField = field;
  }

  public void usingDefaultOperator(QueryOperator operator) {
    defaultOperator = operator;
  }

  //TODO: we omit  public IDocumentQueryCustomization Include<TResult, TInclude>(Expression<Func<TResult, object>> path)
  @Override
  public IDocumentQueryCustomization include(Path<?> path) {
    include(ExpressionExtensions.toPropertyPath(path));
    return this;
  }
  protected QueryOperation initializeQueryOperation(Action2<String, String> setOperationHeaders) {
    IndexQuery indexQuery = getIndexQuery();

    if(beforeQueryExecutionAction != null) {
      beforeQueryExecutionAction.apply(indexQuery);
    }
    /*TODO
    return new QueryOperation(theSession,
                  indexName,
                  indexQuery,
                  projectionFields,
                  sortByHints,
                  theWaitForNonStaleResults,
                  setOperationHeaders,
                  timeout,
                  transformResultsFunc,
                  includes,
                  disableEntitiesTracking); */
    return null; //TODO: delete me
  }

  public IndexQuery getIndexQuery() {
    String query = queryText.toString();
    IndexQuery indexQuery = generateIndexQuery(query);
    return indexQuery;
  }

  public FacetResults getFacets(String facetSetupDoc, int facetStart, Integer facetPageSize) {
    IndexQuery q = getIndexQuery();
    return getDatabaseCommands().getFacets(indexName, q, facetSetupDoc, facetStart, facetPageSize);
  }

  public FacetResults getFacets(List<Facet> facets, int facetStart, Integer facetPageSize) {
    IndexQuery q = getIndexQuery();
    return getDatabaseCommands().getFacets(indexName, q, facets, facetStart, facetPageSize);
  }

  /**
   *  Gets the query result
   *  Execute the query the first time that this is called.
   * @return The query result.
   */
  public QueryResult getQueryResult() {
    initSync();
    return queryOperation.getCurrentQueryResults().createSnapshot();
  }

  protected void InitSync() {
    if (queryOperation != null) {
      return;
    }
    theSession.incrementRequestCount();
    clearSortHints(getDatabaseCommands());
    executeBeforeQueryListeners();
    queryOperation = initializeQueryOperation(DatabaseCommands.OperationsHeaders.Set);
    executeActualQuery();
  }

  protected void clearSortHints(IDatabaseCommands dbCommands) {
    Set<String> keys = new HashSet<>();
    for (String key: dbCommands.getOperationsHeaders().keySet()) {
      if (key.startsWith("SortHint")) {
        keys.add(key);
      }
    }
    for (String key: keys) {
      dbCommands.getOperationsHeaders().remove(key);
    }
  }

  protected void executeActualQuery() {
    /*TODO
    while (true) {
      using (queryOperation.EnterQueryContext())
      {
        queryOperation.LogQuery();
        var result = DatabaseCommands.Query(indexName, queryOperation.IndexQuery, includes.ToArray());
        if (queryOperation.IsAcceptable(result) == false)
        {
          ThreadSleep.Sleep(100);
          continue;
        }
        break;
      }
    }
    invokeAfterQueryExecuted(queryOperation.CurrentQueryResults); */
  }

  /**
   * Register the query as a lazy query in the session and return a lazy
   *  instance that will evaluate the query only when needed
   * @return
   */
  public Lazy<Collection<T>> lazily() {
    return lazily(null);
  }

  /**
   * Register the query as a lazy query in the session and return a lazy
   * instance that will evaluate the query only when needed
   * @param onEval
   * @return
   */
  public Lazy<Collection<T>> lazily(Action1<Collection<T>> onEval) {
    Map<String, String> headers = new HashMap<>();
    if (queryOperation == null) {
      executeBeforeQueryListeners();
      queryOperation = initializeQueryOperation((key, val) => headers[key] =val);
    }

    LazyQueryOperation lazyQueryOperation = new LazyQueryOperation<T>(queryOperation, afterQueryExecutedCallback, includes);
    lazyQueryOperation.SetHeaders(headers);

    return ((DocumentSession)theSession).addLazyOperation(lazyQueryOperation, onEval);
  }

  protected void executeBeforeQueryListeners() {
    for (IDocumentQueryListener documentQueryListener : queryListeners) {
      documentQueryListener.beforeQueryExecuted(this);
    }
  }

  /**
   * Gets the fields for projection
   * @return
   */
  public Collection<String> getProjectionFields() {
    return (projectionFields != null) ? Arrays.asList(projectionFields) : Collections.<String> emptyList();
  }

  /**
   * Order the search results randomly
   */
  @Override
  public IDocumentQueryCustomization randomOrdering() {
    addOrder(Constants.RANDOM_FIELD_NAME + ";" + UUID.randomUUID(), false);
    return this;
  }


  /**
   * Order the search results randomly using the specified seed
   * this is useful if you want to have repeatable random queries
   */
  @Override
  public IDocumentQueryCustomization randomOrdering(String seed) {
    addOrder(Constants.RANDOM_FIELD_NAME + ";" + seed, false);
    return this;
  }

  public IDocumentQueryCustomization beforeQueryExecution(Action1<IndexQuery> action) {
    beforeQueryExecutionAction = Delegates.combine(beforeQueryExecutionAction, action);
    return this;
  }

  @Override
  public IDocumentQueryCustomization transformResults(Function2<IndexQuery,Collection<Object>, Collection<Object>> resultsTransformer) {
    this.transformResultsFunc = resultsTransformer;
    return this;
  }

  @Override
  public IDocumentQueryCustomization highlight(String fieldName, int fragmentLength, int fragmentCount, String fragmentsField) {
    highlightedFields.add(new HighlightedField(fieldName, fragmentLength, fragmentCount, fragmentsField));
    return this;
  }

  @Override
  public IDocumentQueryCustomization highlight(String fieldName, int fragmentLength, int fragmentCount, Reference<FieldHighlightings> fieldHighlightings) {
    highlightedFields.add(new HighlightedField(fieldName, fragmentLength, fragmentCount, null));
    fieldHighlightings.value = highlightings.addField(fieldName);
    return this;
  }

  public IDocumentQueryCustomization setHighlighterTags(String preTag, String postTag) {
    this.setHighlighterTags(new String[] { preTag},  new String[] { postTag} );
    return this;
  }

  public IDocumentQueryCustomization setHighlighterTags(String[] preTags, String[] postTags) {
    highlighterPreTags = preTags;
    highlighterPostTags = postTags;
    return this;
  }

  public IDocumentQueryCustomization noTracking() {
    disableEntitiesTracking = true;
    return this;
  }

  public IDocumentQueryCustomization noCaching() {
    disableCaching = true;
    return this;
  }

  /**
   * Adds an ordering for a specific field to the query
   * @param fieldName Name of the field.
   * @param descending If set to true [descending]
   */
  public void addOrder(String fieldName, boolean descending) {
    addOrder(fieldName, descending, null);
  }

  /**
   * Adds an ordering for a specific field to the query and specifies the type of field for sorting purposes
   * @param fieldName Name of the field.
   * @param descending If set to true [descending]
   * @param fieldType The type of the field to be sorted.
   */
  public void addOrder(String fieldName, boolean descending, Class<?> fieldType) {
    WhereParams whereParamas = new WhereParams();
    whereParamas.setFieldName(fieldName);
    fieldName = ensureValidFieldName(whereParamas);
    fieldName = descending ? "-" + fieldName : fieldName;
    orderByFields = (String[]) ArrayUtils.add(orderByFields, fieldName);
    sortByHints.add(new Tuple<String, Class<?>>(fieldName, fieldType));
  }

  //TODO public virtual IEnumerator<T> GetEnumerator() {

  @Override
  public void take(int count) {
    pageSize = count;
  }

  @Override
  public void skip(int count) {
    start = count;
  }

  public void where(String whereClause) {
    appendSpaceIfNeeded(queryText.length() > 0 && queryText.charAt(queryText.length() - 1) != '(');
    queryText.append(whereClause);
  }

  private void appendSpaceIfNeeded(boolean shouldAppendSpace) {
    if (shouldAppendSpace) {
      queryText.append(" ");
    }
  }

  /**
   * Matches exact value
   * @param fieldName
   * @param value
   */
  public void whereEquals(String fieldName, Object value) {
    WhereParams whereParams = new WhereParams();;
    whereParams.setFieldName(fieldName);
    whereParams.setValue(value);
    whereEquals(whereParams);
  }

  public void whereEquals(String fieldName, Object value, boolean isAnalyzed) {
    WhereParams whereParams = new WhereParams();
    whereParams.setAllowWildcards(isAnalyzed);
    whereParams.setAnalyzed(isAnalyzed);
    whereParams.setFieldName(fieldName);
    whereParams.setValue(value);
    whereEquals(whereParams);
  }

  /**
   * Simplified method for opening a new clause within the query
   */
  public void openSubclause() {
    currentClauseDepth++;
    appendSpaceIfNeeded(queryText.length() > 0 && queryText.charAt(queryText.length() - 1) != '(');
    negateIfNeeded();
    queryText.append("(");
  }

  /**
   * Instruct the index to group by the specified fields using the specified aggregation operation
   *
   * This is only valid on dynamic indexes queries
   * @param aggregationOperation
   * @param string
   */
  public void groupBy(AggregationOperation aggregationOperation, String... fieldsToGroupBy) {
    groupByFields = fieldsToGroupBy;
    aggregationOp = aggregationOperation;
  }

  /**
   * Simplified method for closing a clause within the query
   */
  public void closeSubclause() {
    currentClauseDepth--;
    queryText.append(")");
  }

  /**
   * Matches exact value
   * @param whereParams
   */
  public void whereEquals(WhereParams whereParams) {
    ensureValidFieldName(whereParams);
    String transformToEqualValue = transformToEqualValue(whereParams);
    lastEquality = new Tuple<String, String>(whereParams.getFieldName(), transformToEqualValue);

    appendSpaceIfNeeded(queryText.length() > 0 && queryText.charAt(queryText.length() - 1) != '(');
    negateIfNeeded();

    queryText.append(RavenQuery.escapeField(whereParams.getFieldName()));
    queryText.append(":");
    queryText.append(transformToEqualValue);
  }

  private String ensureValidFieldName(WhereParams whereParams) {
    if (theSession == null || theSession.getConventions() == null || whereParams.isNestedPath() || isMapReduce) {
      return whereParams.getFieldName();
    }

    /*TODO
    foreach (var rootType in rootTypes)
    {
      var identityProperty = theSession.Conventions.GetIdentityProperty(rootType);
      if (identityProperty != null && identityProperty.Name == whereParams.FieldName)
      {
        whereParams.FieldTypeForIdentifier = rootType;
        return whereParams.FieldName = Constants.DocumentIdFieldName;
      }
    }*/

    return whereParams.getFieldName();
  }

  /**
   * Negate the next operation
   */
  public void negateNext() {
    negate = !negate;
  }

  private void negateIfNeeded() {
    if (negate == false) {
      return;
    }
    negate = false;
    queryText.append("-");
  }

  /**
   * Check that the field has one of the specified value
   * @param fieldName
   * @param values
   */
  @SuppressWarnings("unchecked")
  public IDocumentQuery<T> whereIn(String fieldName, Collection<Object> values) {
    appendSpaceIfNeeded(queryText.length() > 0 && !Character.isWhitespace(queryText.charAt(queryText.length() - 1)));
    negateIfNeeded();

    Collection<Object> list = new ArrayList<>(values);

    if(list.size() == 0) {
      queryText.append("@emptyIn<")
      .append(fieldName)
      .append(">:(no-results)");
      return (IDocumentQuery<T>) this;
    }

    queryText.append("@in<")
    .append(fieldName)
    .append(">:(");

    boolean first = true;
    addItemToInClause(fieldName, list, first);
    queryText.append(") ");
    return (IDocumentQuery<T>) this;
  }

  private void addItemToInClause(String fieldName, Collection<Object> list, boolean first) {
    for (Object value : list) {
      if (value instanceof Collection) {
        addItemToInClause(fieldName, (Collection<Object>)value, first);
        return ;
      }
      if (first == false) {
        queryText.append(",");
      }
      first = false;
      WhereParams whereParams = new WhereParams();
      whereParams.setAllowWildcards(true);
      whereParams.setAnalyzed(true);
      whereParams.setFieldName(fieldName);
      whereParams.setValue(value);
      ensureValidFieldName(whereParams);
      queryText.append(StringUtils.replace(transformToEqualValue(whereParams), ",", "`,`"));
    }
  }

  /**
   * Matches fields which starts with the specified value.
   * @param fieldName Name of the field.
   * @param value The value.s
   */
  public void whereStartsWith(String fieldName, Object value) {
    // NOTE: doesn't fully match startsWith semantics
    WhereParams whereParams = new WhereParams();
    whereParams.setFieldName(fieldName);
    whereParams.setValue(value.toString() + "*");
    whereParams.setAnalyzed(true);
    whereParams.setAllowWildcards(true);
    whereEquals(whereParams);
  }

  /**
   * Matches fields which ends with the specified value.
   * @param fieldName Name of the field
   * @param value The value.
   */
  public void whereEndsWith(String fieldName, Object value) {
    // http://lucene.apache.org/java/2_4_0/queryparsersyntax.html#Wildcard%20Searches
    // You cannot use a * or ? symbol as the first character of a search

    // NOTE: doesn't fully match EndsWith semantics
    WhereParams whereParams = new WhereParams();
    whereParams.setFieldName(fieldName);
    whereParams.setValue("*" + value.toString());
    whereParams.setAllowWildcards(true);
    whereParams.setAnalyzed(true);
    whereEquals(whereParams);
  }

  /**
   * Matches fields where the value is between the specified start and end, exclusive
   * @param fieldName Name of the field.
   * @param start The start.
   * @param end The end.
   */
  public void whereBetween(String fieldName, Object start, Object end) {
    appendSpaceIfNeeded(queryText.length() > 0);

    if ((start != null? start : end) != null)
      sortByHints.add(new Tuple<String, Class<?>>(fieldName, (start != null? start : end).getClass()));

    negateIfNeeded();

    fieldName = getFieldNameForRangeQueries(fieldName, start, end);

    queryText.append(RavenQuery.escapeField(fieldName)).Append(":{");
    WhereParams startParams = new WhereParams();
    startParams.setValue(start);
    startParams.setFieldName(fieldName);

    WhereParams endParams = new WhereParams();
    endParams.setValue(end);
    endParams.setFieldName(fieldName);
    queryText.append(start == null ? "*" : transformToRangeValue(startParams));
    queryText.append(" TO ");
    queryText.append(end == null ? "NULL" : transformToRangeValue(endParams));
    queryText.append("}");
  }




}
