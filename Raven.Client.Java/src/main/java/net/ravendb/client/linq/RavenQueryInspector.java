package net.ravendb.client.linq;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import net.ravendb.abstractions.LinqOps;
import net.ravendb.abstractions.basic.Lazy;
import net.ravendb.abstractions.basic.Reference;
import net.ravendb.abstractions.basic.Tuple;
import net.ravendb.abstractions.closure.Action1;
import net.ravendb.abstractions.data.Facet;
import net.ravendb.abstractions.data.FacetResults;
import net.ravendb.abstractions.data.IndexQuery;
import net.ravendb.abstractions.data.QueryResult;
import net.ravendb.abstractions.data.SuggestionQuery;
import net.ravendb.abstractions.data.SuggestionQueryResult;
import net.ravendb.abstractions.extensions.ExpressionExtensions;
import net.ravendb.abstractions.json.linq.RavenJToken;
import net.ravendb.abstractions.json.linq.RavenJValue;
import net.ravendb.client.EscapeQueryOptions;
import net.ravendb.client.IDocumentQuery;
import net.ravendb.client.RavenQueryHighlightings;
import net.ravendb.client.RavenQueryStatistics;
import net.ravendb.client.SearchOptions;
import net.ravendb.client.SearchOptionsSet;
import net.ravendb.client.connection.IDatabaseCommands;
import net.ravendb.client.connection.IRavenQueryInspector;
import net.ravendb.client.document.DocumentQueryCustomizationFactory;
import net.ravendb.client.document.DocumentSession;
import net.ravendb.client.document.InMemoryDocumentSessionOperations;
import net.ravendb.client.document.LazyFacetsOperation;
import net.ravendb.client.document.batches.LazySuggestOperation;
import net.ravendb.client.indexes.AbstractTransformerCreationTask;
import net.ravendb.client.spatial.SpatialCriteria;

import org.apache.commons.lang.StringUtils;

import com.mysema.query.support.Expressions;
import com.mysema.query.types.Expression;
import com.mysema.query.types.OrderSpecifier;
import com.mysema.query.types.Path;
import com.mysema.query.types.Predicate;
import com.mysema.query.types.expr.BooleanExpression;


public class RavenQueryInspector<T> implements IRavenQueryable<T>, IRavenQueryInspector {

  private Class<T> clazz;
  private final Expression<?> expression;
  private IRavenQueryProvider provider;
  private final RavenQueryStatistics queryStats;
  private final RavenQueryHighlightings highlightings;
  private final String indexName;
  private final IDatabaseCommands databaseCommands;
  private InMemoryDocumentSessionOperations session;
  private final boolean isMapReduce;

  public RavenQueryInspector(Class<T> clazz, IRavenQueryProvider provider, RavenQueryStatistics queryStats, RavenQueryHighlightings highlightings, String indexName, Expression<?> expression, InMemoryDocumentSessionOperations session, IDatabaseCommands databaseCommands, boolean isMapReduce) {
    this.clazz = clazz;
    if (provider == null) {
      throw new IllegalArgumentException("provider is null");
    }
    this.provider = provider.forClass(clazz);
    this.queryStats = queryStats;
    this.highlightings = highlightings;
    this.indexName = indexName;
    this.session = session;
    this.databaseCommands = databaseCommands;
    this.isMapReduce = isMapReduce;
    this.provider.afterQueryExecuted(new Action1<QueryResult>() {

      @Override
      public void apply(QueryResult queryResult) {
        afterQueryExecuted(queryResult);
      }
    });
    this.expression = expression != null ? expression : Expressions.constant("root");
  }

  private void afterQueryExecuted(QueryResult queryResult) {
    this.queryStats.updateQueryStats(queryResult);
    this.highlightings.update(queryResult);
  }

  @Override
  public Expression< ? > getExpression() {
    return expression;
  }

  @Override
  public Class< ? > getElementType() {
    return clazz;
  }

  @Override
  public IQueryProvider getProvider() {
    return provider;
  }

  @Override
  public IRavenQueryable<T> where(Predicate predicate) {
    return getProvider().createQuery(Expressions.operation(expression.getType(), LinqOps.Query.WHERE, expression, predicate));
  }

  /**
   * Provide statistics about the query, such as total count of matching records
   */
  @Override
  public IRavenQueryable<T> statistics(Reference<RavenQueryStatistics> stats) {
    stats.value = queryStats;
    return this;
  }

  /**
   * Customizes the query using the specified action
   */
  @Override
  public IRavenQueryable<T> customize(DocumentQueryCustomizationFactory factory) {
    provider.customize(factory);
    return this;
  }

  @Override
  public <S> IRavenQueryable<S> transformWith(Class< ? extends AbstractTransformerCreationTask> transformerClazz, Class<S> resultClass) {
    try {
      AbstractTransformerCreationTask transformer = transformerClazz.newInstance();
      provider.transformWith(transformer.getTransformerName());
      return as(resultClass);
    } catch (Exception e){
      throw new RuntimeException(e);
    }
  }


  @Override
  public IRavenQueryable<T> addQueryInput(String name, RavenJToken value) {
    provider.addQueryInput(name, value);
    return this;
  }

  @Override
  public IRavenQueryable<T> addQueryInput(String name, Object value) {
    provider.addQueryInput(name, new RavenJValue(value));
    return this;
  }

  @Override
  public IRavenQueryable<T> spatial(final Path< ? > path, final SpatialCriteria criteria) {
    return customize(new DocumentQueryCustomizationFactory().spatial(ExpressionExtensions.toPropertyPath(path), criteria));
  }

  @Override
  public String toString() {
    RavenQueryProviderProcessor<T> ravenQueryProvider = getRavenQueryProvider();
    IDocumentQuery<T> luceneQuery = ravenQueryProvider.getLuceneQueryFor(expression);
    String fields = "";
    if (!ravenQueryProvider.getFieldsToFetch().isEmpty()) {
      fields = "<" + StringUtils.join(ravenQueryProvider.getFieldsToFetch(), ", ") + ">: ";
    }
    return fields + luceneQuery;
  }

  @Override
  public IndexQuery getIndexQuery() {
    RavenQueryProviderProcessor<T> ravenQueryProvider = getRavenQueryProvider();
    IDocumentQuery<T> luceneQuery = ravenQueryProvider.getLuceneQueryFor(expression);
    return luceneQuery.getIndexQuery();
  }

  @Override
  public FacetResults getFacets(String facetSetupDoc, int start, Integer pageSize) {
    return databaseCommands.getFacets(indexName, getIndexQuery(), facetSetupDoc, start, pageSize);
  }

  @Override
  public FacetResults getFacets(List<Facet> facets, int start, Integer pageSize) {
    return databaseCommands.getFacets(indexName, getIndexQuery(), facets, start, pageSize);
  }

  private RavenQueryProviderProcessor<T> getRavenQueryProvider() {
    return new RavenQueryProviderProcessor<>(clazz, provider.getQueryGenerator(), provider.getCustomizeQuery(), null, indexName,
        new HashSet<String>(), new ArrayList<RenamedField>(), isMapReduce, provider.getResultTranformer(), provider.getQueryInputs());
  }

  @Override
  public String getIndexQueried() {
    RavenQueryProviderProcessor<T> ravenQueryProvider = new RavenQueryProviderProcessor<>(clazz, provider.getQueryGenerator(), null, null, indexName, new HashSet<String>(),
        new ArrayList<RenamedField>(), isMapReduce, provider.getResultTranformer(), provider.getQueryInputs());
    IDocumentQuery<T> luceneQuery = ravenQueryProvider.getLuceneQueryFor(expression);
    return ((IRavenQueryInspector)luceneQuery).getIndexQueried();
  }

  @Override
  public IDatabaseCommands getDatabaseCommands() {
    if (databaseCommands == null) {
      throw new IllegalStateException("You cannot get database commands for this query");
    }
    return databaseCommands;
  }

  @Override
  public InMemoryDocumentSessionOperations getSession() {
    return session;
  }

  @Override
  public Tuple<String, String> getLastEqualityTerm() {
    RavenQueryProviderProcessor<T> ravenQueryProvider = new RavenQueryProviderProcessor<>(clazz, provider.getQueryGenerator(), null, null, indexName, new HashSet<String>(),
        new ArrayList<RenamedField>(), isMapReduce, provider.getResultTranformer(), provider.getQueryInputs());
    IDocumentQuery<T> luceneQuery = ravenQueryProvider.getLuceneQueryFor(expression);
    return ((IRavenQueryInspector) luceneQuery).getLastEqualityTerm();
  }

  public void fieldsToFetch(List<String> fields) {
    for (String field: fields) {
      provider.getFieldsToFetch().add(field);
    }
  }

  @Override
  public Lazy<List<T>> lazily() {
    return provider.lazily(clazz, expression, null);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <S> IRavenQueryable<S> as(Class<S> resultClass) {
    this.clazz = (Class<T>) resultClass;
    this.provider = provider.forClass(resultClass);
    return (IRavenQueryable<S>) this;
  }

  @Override
  public IRavenQueryable<T> orderBy(OrderSpecifier< ? >... asc) {
    return getProvider().createQuery(Expressions.operation(expression.getType(), LinqOps.Query.ORDER_BY, expression, Expressions.constant(asc)));
  }

  @Override
  public DynamicAggregationQuery<T> aggregateBy(String path) {
    return new DynamicAggregationQuery<>(this, path);
  }

  @Override
  public DynamicAggregationQuery<T> aggregateBy(String path, String displayName) {
    return new DynamicAggregationQuery<>(this, path, displayName);
  }

  @Override
  public DynamicAggregationQuery<T> aggregateBy(Path< ? > path) {
    return new DynamicAggregationQuery<>(this, path);
  }

  @Override
  public DynamicAggregationQuery<T> aggregateBy(Path< ? > path, String displayName) {
    return new DynamicAggregationQuery<>(this, path, displayName);
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
    List<Facet> facetsList = facets;
    if (facetsList.isEmpty()) {
      throw new IllegalArgumentException("Facets must contain at least one entry");
    }
    IndexQuery query = getIndexQuery();

    LazyFacetsOperation lazyOperation = new LazyFacetsOperation(getIndexQueried(), facetsList, query, start, pageSize);
    DocumentSession session = (DocumentSession) getSession();
    return session.addLazyOperation(lazyOperation, null);
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
    IndexQuery query = getIndexQuery();

    LazyFacetsOperation lazyOperation = new LazyFacetsOperation(getIndexQueried(), facetSetupDoc, query, start, pageSize);
    DocumentSession documentSession = (DocumentSession) getSession();
    return documentSession.addLazyOperation(lazyOperation, null);
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
    return databaseCommands.getFacets(indexName, getIndexQuery(), facets, start, pageSize);
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
    return databaseCommands.getFacets(indexName, getIndexQuery(), facetSetupDoc, start, pageSize);
  }

  @Override
  public IRavenQueryable<T> include(Path< ? > path) {
    customize(new DocumentQueryCustomizationFactory().include(path));
    return this;
  }

  @Override
  public IRavenQueryable<T> intersect() {
    return provider.createQuery(Expressions.operation(Object.class, LinqOps.Query.INTERSECT, getExpression()));
  }

  @Override
  public SuggestionQueryResult suggest() {
    return suggest(new SuggestionQuery());
  }

  @Override
  public SuggestionQueryResult suggest(SuggestionQuery query) {
    setSuggestionQueryFieldAndTerm(this, query);
    return getDatabaseCommands().suggest(getIndexQueried(), query);
  }

  @Override
  public Lazy<SuggestionQueryResult> suggestLazy() {
    return suggestLazy(new SuggestionQuery());
  }

  @Override
  public Lazy<SuggestionQueryResult> suggestLazy(SuggestionQuery query) {
    setSuggestionQueryFieldAndTerm(this, query);
    LazySuggestOperation lazyOperation = new LazySuggestOperation(getIndexQueried(), query);

    DocumentSession documentSession = (DocumentSession) getSession();
    return documentSession.addLazyOperation(lazyOperation, null);
  }

  private static void setSuggestionQueryFieldAndTerm(IRavenQueryInspector queryInspector, SuggestionQuery query) {
    if (StringUtils.isNotEmpty(query.getField()) && StringUtils.isNotEmpty(query.getTerm())) {
      return;
    }
    Tuple<String, String> lastEqualityTerm = queryInspector.getLastEqualityTerm();
    if (lastEqualityTerm.getItem1() == null) {
      throw new IllegalStateException("Could not suggest on a query that doesn't have a single equality check");
    }
    query.setField(lastEqualityTerm.getItem1());
    query.setTerm(lastEqualityTerm.getItem2());
  }

  @Override
  public Lazy<List<T>> lazily(Action1<List<T>> onEval) {
    return provider.lazily(clazz, getExpression(), onEval);
  }

  @Override
  public IRavenQueryable<T> search(Path< ? > fieldSelector, String searchTerms) {
    return search(fieldSelector, searchTerms, 1.0, new SearchOptionsSet(SearchOptions.GUESS), EscapeQueryOptions.ESCAPE_ALL);
  }

  @Override
  public IRavenQueryable<T> search(Path< ? > fieldSelector, String searchTerms, double boost) {
    return search(fieldSelector, searchTerms, boost, new SearchOptionsSet(SearchOptions.GUESS), EscapeQueryOptions.ESCAPE_ALL);
  }

  @Override
  public IRavenQueryable<T> search(Path< ? > fieldSelector, String searchTerms, double boost, SearchOptionsSet searchOptions) {
    return search(fieldSelector, searchTerms, boost, searchOptions, EscapeQueryOptions.ESCAPE_ALL);
  }

  @Override
  public IRavenQueryable<T> search(Path< ? > fieldSelector, String searchTerms, double boost, SearchOptionsSet options, EscapeQueryOptions escapeQueryOptions) {
    // we use constant null to preserve arguments indexes
    return provider.createQuery(Expressions.operation(Object.class, LinqOps.Query.SEARCH, getExpression(), fieldSelector,
        Expressions.constant(searchTerms), Expressions.constant(boost), Expressions.constant(options), Expressions.constant(escapeQueryOptions)));
  }

  @Override
  public IRavenQueryable<T> orderByScore() {
    return provider.createQuery(Expressions.operation(Object.class, LinqOps.Query.ORDER_BY_SCORE, getExpression()));
  }

  @Override
  public IRavenQueryable<T> skip(int itemsToSkip) {
    return provider.createQuery(Expressions.operation(Object.class, LinqOps.Query.SKIP, getExpression(), Expressions.constant(itemsToSkip)));
  }

  @Override
  public IRavenQueryable<T> take(int amount) {
    return provider.createQuery(Expressions.operation(Object.class, LinqOps.Query.TAKE, getExpression(), Expressions.constant(amount)));
  }

  @SuppressWarnings("unchecked")
  @Override
  public Iterator<T> iterator() {
    Object execute = provider.execute(expression);
    return ((Iterable<T>) execute).iterator();
  }

  @Override
  public T firstOrDefault() {
    return (T) provider.execute(Expressions.operation(Object.class, LinqOps.Query.FIRST_OR_DEFAULT, getExpression()));
  }

  @Override
  public List<T> toList() {
    return EnumerableUtils.toList(iterator());
  }

  @Override
  public T first() {
    return (T) provider.execute(Expressions.operation(Object.class, LinqOps.Query.FIRST, getExpression()));
  }

  @Override
  public T singleOrDefault() {
    return (T) provider.execute(Expressions.operation(Object.class, LinqOps.Query.SINGLE_OR_DEFAULT, getExpression()));
  }

  @Override
  public T single() {
    return (T) provider.execute(Expressions.operation(Object.class, LinqOps.Query.SINGLE, getExpression()));
  }

  @Override
  public int count() {
    return (int) provider.execute(Expressions.operation(Object.class, LinqOps.Query.COUNT, getExpression()));
  }

  @Override
  public long longCount() {
    return (long) provider.execute(Expressions.operation(Object.class, LinqOps.Query.LONG_COUNT, getExpression()));
  }

  @Override
  public IRavenQueryable<T> distinct() {
    return provider.createQuery(Expressions.operation(Object.class, LinqOps.Query.DISTINCT, getExpression()));
  }

  @Override
  public <TProjection> IRavenQueryable<TProjection> select(Class<TProjection> projectionClass) {
    return provider.createQuery(Expressions.operation(Object.class, LinqOps.Query.SELECT, getExpression(), Expressions.constant(projectionClass) )).as(projectionClass);
  }

  @Override
  public <TProjection> IRavenQueryable<TProjection> select(Path<TProjection> projectionPath) {
    return (IRavenQueryable<TProjection>) provider.createQuery(Expressions.operation(Object.class, LinqOps.Query.SELECT, getExpression(), projectionPath )).as(projectionPath.getType());
  }

  @Override
  public <TProjection> IRavenQueryable<TProjection> select(Class<TProjection> projectionClass, String... fields) {
    return provider.createQuery(Expressions.operation(Object.class, LinqOps.Query.SELECT, getExpression(),
        Expressions.constant(projectionClass), Expressions.constant(fields), Expressions.constant(fields))).as(projectionClass);
  }

  @Override
  public <TProjection> IRavenQueryable<TProjection> select(Class<TProjection> projectionClass, String[] fields, String[] projections) {
    return provider.createQuery(Expressions.operation(Object.class, LinqOps.Query.SELECT, getExpression(),
        Expressions.constant(projectionClass), Expressions.constant(fields), Expressions.constant(projections))).as(projectionClass);
  }

  @Override
  public <TProjection> IRavenQueryable<TProjection> select(Class<TProjection> projectionClass, Path< ? >... fields) {
    List<String> fieldsAsString = new ArrayList<>();
    for (Path<?> path : fields) {
      fieldsAsString.add(ExpressionExtensions.toPropertyPath(path));
    }
    return select(projectionClass, fieldsAsString.toArray(new String[0]));
  }

  @Override
  public <TProjection> IRavenQueryable<TProjection> select(Class<TProjection> projectionClass, Path< ? >[] fields, Path< ? >[] projections) {
    List<String> fieldsAsString = new ArrayList<>();
    for (Path<?> path : fields) {
      fieldsAsString.add(ExpressionExtensions.toPropertyPath(path));
    }
    List<String> projectionsAsString = new ArrayList<>();
    for (Path<?> path : projections) {
      projectionsAsString.add(ExpressionExtensions.toPropertyPath(path));
    }
    return select(projectionClass, fieldsAsString.toArray(new String[0]), projectionsAsString.toArray(new String[0]));
  }

  @Override
  public T first(BooleanExpression condition) {
    return where(condition).first();
  }

  @Override
  public T firstOrDefault(BooleanExpression predicate) {
    return where(predicate).firstOrDefault();
  }

  @Override
  public T single(BooleanExpression predicate) {
    return where(predicate).single();
  }

  @Override
  public T singleOrDefault(BooleanExpression predicate) {
    return where(predicate).singleOrDefault();
  }

  @Override
  public int count(BooleanExpression predicate) {
    return where(predicate).count();
  }

  @Override
  public long longCount(BooleanExpression predicate) {
    return where(predicate).longCount();
  }

  @Override
  public boolean any() {
    return (boolean) provider.execute(Expressions.operation(Boolean.class, LinqOps.Query.ANY_RESULT, getExpression()));
  }

}
