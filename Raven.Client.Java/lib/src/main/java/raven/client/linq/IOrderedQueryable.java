package raven.client.linq;

import java.util.List;

import com.mysema.query.types.Expression;
import com.mysema.query.types.OrderSpecifier;
import com.mysema.query.types.Path;
import com.mysema.query.types.Predicate;
import com.mysema.query.types.expr.BooleanExpression;
//TODO java doc
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

  public List<T> toList();

  public IRavenQueryable<T> skip(int itemsToSkip);

  public IRavenQueryable<T> take(int amount);

  public T first();

  public T first(BooleanExpression predicate);

  public T firstOrDefault();

  public T firstOrDefault(BooleanExpression predicate);

  public T single();

  public T single(BooleanExpression predicate);

  public T singleOrDefault();

  public T singleOrDefault(BooleanExpression predicate);

  public int count();

  public int count(BooleanExpression predicate);

  public long longCount();

  public long longCount(BooleanExpression predicate);

  public Class<?> getElementType();

  public Expression<?> getExpression();

  public IQueryProvider getProvider();




  /**
   * Project using a different type
   * @param clazz
   * @return
   */
  public <TResult> IRavenQueryable<TResult> as(Class<TResult> clazz);


  //TODO: finish me

}
