package net.ravendb.client.linq;

import java.util.List;
import java.util.Map;
import java.util.Set;

import net.ravendb.abstractions.basic.Lazy;
import net.ravendb.abstractions.closure.Action1;
import net.ravendb.abstractions.data.QueryResult;
import net.ravendb.abstractions.json.linq.RavenJToken;
import net.ravendb.client.IDocumentQuery;
import net.ravendb.client.document.DocumentQueryCustomizationFactory;

import com.mysema.query.types.Expression;


/**
 * Extension for the built-in IQueryProvider allowing for Raven specific operations
 */
public interface IRavenQueryProvider extends IQueryProvider {
  /**
   * Callback to get the results of the query
   * @param afterQueryExecuted
   */
  void afterQueryExecuted(Action1<QueryResult> afterQueryExecuted);

  /**
   * Customizes the query using the specified action
   * @param action
   */
  void customize(DocumentQueryCustomizationFactory factory);

  /**
   * The name of the transformer to use with this query
   * @param transformerName
   */
  void transformWith(String transformerName);

  /**
   * Gets the name of the index.
   * @return
   */
  public String getIndexName();

  /**
   * Get the query generator
   * @return
   */
  public IDocumentQueryGenerator getQueryGenerator();

  /**
   * The action to execute on the customize query
   * @return
   */
  public DocumentQueryCustomizationFactory getCustomizeQuery();

  /**
   * Change the result type for the query provider
   * @param clazz
   * @return
   */
  public <S> IRavenQueryProvider forClass(Class<S> clazz);

  /**
   * Convert the linq query to a Lucene query
   * @param clazz
   * @param expression
   * @return
   */
  public <T> IDocumentQuery<T> toDocumentQuery(Class<T> clazz, Expression<?> expression);

  /**
   * Convert the Linq query to a lazy Lucene query and provide a function to execute when it is being evaluate
   * @param expression
   * @param onEval
   * @return
   */
  public <T> Lazy<List<T>> lazily(Class<T> clazz, Expression<?> expression, Action1<List<T>> onEval);

  public <T> Lazy<Integer> countLazily(Class<T> clazz, Expression<?> expression);

  /**
   * Set the fields to fetch
   * @return
   */
  public Set<String> getFieldsToFetch();

  /**
   * The result transformer to use
   * @return
   */
  public String getResultTranformer();

  /**
   * Gets the query inputs being supplied to
   * @return
   */
  public Map<String, RavenJToken> getQueryInputs();

  /**
   * Adds input to this query via a key/value pair
   * @param input
   * @param foo
   */
  public void addQueryInput(String input, RavenJToken foo);
}
