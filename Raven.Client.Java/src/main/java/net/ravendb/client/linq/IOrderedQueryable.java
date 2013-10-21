package net.ravendb.client.linq;

import java.util.List;

import com.mysema.query.types.Expression;
import com.mysema.query.types.OrderSpecifier;
import com.mysema.query.types.Path;
import com.mysema.query.types.Predicate;
import com.mysema.query.types.expr.BooleanExpression;
public interface IOrderedQueryable<T> extends Iterable<T> {
  /**
   * Filters a sequence of values based on a predicate.
   * @param predicate
   * @return
   */
  public IRavenQueryable<T> where(Predicate predicate);

  /**
   * Projects results
   * @param projectionClass
   * @return
   */
  public <TProjection> IRavenQueryable<TProjection> select(Class<TProjection> projectionClass);

  /**
   * Projects results
   * @param projectionClass
   * @return
   */
  public <TProjection> IRavenQueryable<TProjection> select(Class<TProjection> projectionClass, String... fields);

  /**
   * Projects results
   * @param projectionClass
   * @return
   */
  public <TProjection> IRavenQueryable<TProjection> select(Class<TProjection> projectionClass, String[] fields, String[] projections);

  /**
   * Projects results
   * @param projectionClass
   * @return
   */
  public <TProjection> IRavenQueryable<TProjection> select(Class<TProjection> projectionClass, Path<?>... fields);

  /**
   * Projects results
   * @param projectionClass
   * @return
   */
  public <TProjection> IRavenQueryable<TProjection> select(Class<TProjection> projectionClass, Path<?>[] fields, Path<?>[] projections);


  /**
   * Projects results based on projection path
   * @param projectionPath
   * @return
   */
  public <TProjection> IRavenQueryable<TProjection> select(Path<TProjection> projectionPath);

  /**
   * Changes order of result elements
   * @param asc
   * @return
   */
  public IRavenQueryable<T> orderBy(OrderSpecifier<?>... asc);

  /**
   * Materialize query and returns results as list.
   * @return
   */
  public List<T> toList();

  /**
   * Skips specified number of records.
   * Method is used for paging.
   * @param itemsToSkip
   * @return
   */
  public IRavenQueryable<T> skip(int itemsToSkip);

  /**
   * Takes specified number of records.
   * Method is used for paging.
   * @param amount
   * @return
   */
  public IRavenQueryable<T> take(int amount);

  /**
   * Returns only first entry from result.
   * Throws if zero results was found.
   * @return
   */
  public T first();

  /**
   * Returns only first entry from result which suffices specified predicate.
   * Throws if zero results was found.
   * @param predicate
   * @return
   */
  public T first(BooleanExpression predicate);

  /**
   * Returns first entry from result or default value if none found.
   * @return
   */
  public T firstOrDefault();

  /**
   * Returns first entry from result which suffices specified predicate or default value if none found.
   * @param predicate
   * @return
   */
  public T firstOrDefault(BooleanExpression predicate);

  /**
   * Return value is based on result amount:
   * 2 entries and over: throws exception
   * 1 entry - return it
   * 0 - throws
   * @return
   */
  public T single();

  /**
   * Return value is based on result amount.
   * 2 entries and over: throws exception
   * 1 entry - return it
   * 0 - throws
   * @param predicate
   * @return
   */
  public T single(BooleanExpression predicate);

  /**
   * Return value is based on result amount.
   * 2 entries and over: throws exception
   * 1 entry - return it
   * 0 - returns default value
   * @param predicate
   * @return
   */
  public T singleOrDefault();

  /**
   * Return value is based on result amount.
   * 2 entries and over: throws exception
   * 1 entry - return it
   * 0 - returns default value
   * @param predicate
   * @return
   */
  public T singleOrDefault(BooleanExpression predicate);

  /**
   * Performs count query.
   * @return
   */
  public int count();

  /**
   * Performs any query.
   * Returns true is any entry would be returned in normal query.
   * @return
   */
  public boolean any();

  /**
   * Performs count query - each result must match specified predicate.
   * @param predicate
   * @return
   */
  public int count(BooleanExpression predicate);

  /**
   * Performs count query.
   * @return
   */
  public long longCount();

  /**
   * Performs count query - each result must match specified predicate.
   * @param predicate
   * @return
   */
  public long longCount(BooleanExpression predicate);

  /**
   * Returns element type
   * @return
   */
  public Class<?> getElementType();

  /**
   * Expression created via DSL
   * @return
   */
  public Expression<?> getExpression();

  /**
   * Query provider.
   * @return
   */
  public IQueryProvider getProvider();

  /**
   * Project using a different type
   * @param clazz
   * @return
   */
  public <TResult> IRavenQueryable<TResult> as(Class<TResult> clazz);

}
