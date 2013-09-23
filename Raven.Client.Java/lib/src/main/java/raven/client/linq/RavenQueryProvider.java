package raven.client.linq;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.mysema.query.types.Expression;

import raven.abstractions.basic.Lazy;
import raven.abstractions.closure.Action1;
import raven.abstractions.data.QueryResult;
import raven.abstractions.json.linq.RavenJToken;
import raven.client.IDocumentQuery;
import raven.client.RavenQueryHighlightings;
import raven.client.RavenQueryStatistics;
import raven.client.connection.IDatabaseCommands;
import raven.client.document.DocumentQueryCustomizationFactory;
import raven.client.document.InMemoryDocumentSessionOperations;

public class RavenQueryProvider<T> implements IRavenQueryProvider {

  private Action1<QueryResult> afterQueryExecuted;
  private DocumentQueryCustomizationFactory customizeQuery;
  private final String indexName;
  private final IDocumentQueryGenerator queryGenerator;
  private final RavenQueryStatistics ravenQueryStatistics;
  private final RavenQueryHighlightings highlightings;
  private final IDatabaseCommands databaseCommands;
  private final boolean isMapReduce;
  private final Map<String, RavenJToken> queryInputs = new HashMap<String, RavenJToken>();
  private final Class<T> clazz;

  private Set<String> fieldsToFetch = new HashSet<>();
  private String resultTranformer;
  private List<RenamedField> fieldsToRename;

  public RavenQueryProvider(Class<T> clazz, IDocumentQueryGenerator queryGenerator, String indexName, RavenQueryStatistics ravenQueryStatistics,
      RavenQueryHighlightings highlightings, IDatabaseCommands databaseCommands, boolean isMapReduce) {
    this.clazz = clazz;

    fieldsToFetch = new HashSet<>();
    fieldsToRename = new ArrayList<>();

    this.queryGenerator = queryGenerator;
    this.indexName = indexName;
    this.ravenQueryStatistics = ravenQueryStatistics;
    this.highlightings = highlightings;
    this.databaseCommands = databaseCommands;
    this.isMapReduce = isMapReduce;

  }

  /**
   * Gets the actions for customizing the generated lucene query
   * @return
   */
  public DocumentQueryCustomizationFactory getCustomizedQuery() {
    return customizeQuery;
  }

  /**
   * Gets the name of the index.
   */
  public String getIndexName() {
    return indexName;
  }

  /**
   * Get the query generator
   */
  public IDocumentQueryGenerator getQueryGenerator() {
    return queryGenerator;
  }

  public DocumentQueryCustomizationFactory getCustomizeQuery() {
    return customizeQuery;
  }

  public Set<String> getFieldsToFetch() {
    return fieldsToFetch;
  }

  /**
   * Gets the results transformer to use
   */
  public String getResultTranformer() {
    return resultTranformer;
  }

  public Map<String, RavenJToken> getQueryInputs() {
    return queryInputs;
  }

  public void addQueryInput(String name, RavenJToken value) {
    queryInputs.put(name, value);
  }

  public List<RenamedField> getFieldsToRename() {
    return fieldsToRename;
  }

  public <S> IRavenQueryProvider forClass(Class<S> clazz) {
    if (this.clazz.equals(clazz)) {
      return this;
    }
    RavenQueryProvider<S> ravenQueryProvider = new RavenQueryProvider<>(clazz, queryGenerator, indexName, ravenQueryStatistics, highlightings, databaseCommands, isMapReduce);
    ravenQueryProvider.resultTranformer = resultTranformer;
    ravenQueryProvider.customize(customizeQuery);
    for (Map.Entry<String, RavenJToken> queryInput: queryInputs.entrySet()) {
      ravenQueryProvider.addQueryInput(queryInput.getKey(), queryInput.getValue());
    }
    return ravenQueryProvider;
  }

  public Object execute(Expression<?> expression) {
    return getQueryProviderProcessor(clazz).execute(expression);
  }

  /**
   *  Callback to get the results of the query
   */
  public void afterQueryExecuted(Action1<QueryResult> afterQueryExecutedCallback) {
    this.afterQueryExecuted = afterQueryExecutedCallback;
  }

  /**
   *  Customizes the query using the specified action
   */
  public void customize(DocumentQueryCustomizationFactory factory) {
    if (factory == null) {
      return;
    }
    if (customizeQuery == null) {
      customizeQuery = factory;
    }
    // merge 2 factories
    customizeQuery = DocumentQueryCustomizationFactory.join(customizeQuery, factory);
  }

  public void transformWith(String transformerName) {
    this.resultTranformer = transformerName;
  }


  protected <S> RavenQueryProviderProcessor<S> getQueryProviderProcessor(Class<S> clazz) {
    return new RavenQueryProviderProcessor<S>(clazz, queryGenerator, customizeQuery, afterQueryExecuted, indexName,
        fieldsToFetch, fieldsToRename, isMapReduce, resultTranformer, queryInputs);
  }

  /**
   * Convert the expression to a Lucene query
   */
  @SuppressWarnings("unchecked")
  public <S> IDocumentQuery<S> toLuceneQuery(Class<S> clazz, Expression<?> expression) {
    RavenQueryProviderProcessor<T> processor = getQueryProviderProcessor(this.clazz);
    IDocumentQuery<S> result = (IDocumentQuery<S>) processor.getLuceneQueryFor(expression);
    result.setResultTransformer(resultTranformer);
    return result;
  }

  @Override
  public <S> Lazy<List<S>> lazily(Class<S> clazz, Expression< ? > expression, Action1<List<S>> onEval) {
    final RavenQueryProviderProcessor<S> processor = getQueryProviderProcessor(clazz);
    IDocumentQuery<S> query = processor.getLuceneQueryFor(expression);
    if (afterQueryExecuted != null) {
      query.afterQueryExecuted(afterQueryExecuted);
    }

    List<String> renamedFields = new ArrayList<>();
    outer:
    for (String field :fieldsToFetch) {
      for (RenamedField renamedField : fieldsToRename) {
        if (renamedField.getOriginalField().equals(field)) {
          renamedFields.add(renamedField.getNewField() != null ? renamedField.getNewField() : field);
          continue outer;
        }
      }
      renamedFields.add(field);
    }

    if (!renamedFields.isEmpty()) {
      query.afterQueryExecuted(new Action1<QueryResult>() {
        @Override
        public void apply(QueryResult queryResult) {
          processor.renameResults(queryResult);
        }
      });
    }
    if (!fieldsToFetch.isEmpty()) {
      query = query.selectFields(clazz, fieldsToFetch.toArray(new String[0]), renamedFields.toArray(new String[0]));
    }
    return query.lazily(onEval);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <S> IRavenQueryable<S> createQuery(Expression< ? > expression) {
    return new RavenQueryInspector<S>((Class<S>) clazz, this,
        ravenQueryStatistics, highlightings, indexName, expression, (InMemoryDocumentSessionOperations) queryGenerator, databaseCommands, isMapReduce);

  }

}
