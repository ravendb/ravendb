package raven.client.linq;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.mysema.query.support.Expressions;
import com.mysema.query.types.Expression;
import com.mysema.query.types.OrderSpecifier;
import com.mysema.query.types.Path;
import com.mysema.query.types.Predicate;

import raven.abstractions.basic.Lazy;
import raven.abstractions.basic.Reference;
import raven.abstractions.basic.Tuple;
import raven.abstractions.closure.Action1;
import raven.abstractions.closure.Function1;
import raven.abstractions.data.Facet;
import raven.abstractions.data.FacetResults;
import raven.abstractions.data.IndexQuery;
import raven.abstractions.data.QueryResult;
import raven.abstractions.extensions.ExpressionExtensions;
import raven.abstractions.json.linq.RavenJToken;
import raven.client.IDocumentQuery;
import raven.client.IDocumentQueryCustomization;
import raven.client.RavenQueryHighlightings;
import raven.client.RavenQueryStatistics;
import raven.client.connection.IDatabaseCommands;
import raven.client.connection.IRavenQueryInspector;
import raven.client.document.InMemoryDocumentSessionOperations;
import raven.client.indexes.AbstractTransformerCreationTask;
import raven.client.spatial.SpatialCriteria;
import raven.client.spatial.SpatialCriteriaFactory;
import raven.linq.dsl.LinqOps;

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

  @SuppressWarnings("unchecked")
  @Override
  public List<T> toList() {
    Object execute = getProvider().execute(expression);
    return ((IDocumentQuery<T>)execute).toList();
  }

  @SuppressWarnings("unchecked")
  @Override
  public T single() {
    Object execute = getProvider().execute(expression);
    List<T> list = ((IDocumentQuery<T>)execute).toList();
    if (list.size() != 1) {
      throw new IllegalStateException("Expected single result! Got: " + list.size());
    }
    return list.get(0);
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
  public IRavenQueryable<T> customize(Action1<IDocumentQueryCustomization> action) {
    provider.customize(action);
    return this;
  }

  @Override
  public <S> IRavenQueryable<S> transformWith(Class< ? extends AbstractTransformerCreationTask> transformerClazz, Class<S> resultClass) {
    try {
      AbstractTransformerCreationTask transformer = transformerClazz.newInstance();
      provider.transformWith(transformer.getTransformerName());
      return (IRavenQueryable<S>) as(resultClass);
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
  public IRavenQueryable<T> spatial(final Path< ? > path, final Function1<SpatialCriteriaFactory, SpatialCriteria> clause) {
    return customize(new Action1<IDocumentQueryCustomization>() {

      @Override
      public void apply(IDocumentQueryCustomization x) {
        x.spatial(ExpressionExtensions.toPropertyPath(path), clause);
      }
    });
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


}
