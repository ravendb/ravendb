package raven.client.linq;

import java.util.List;

import raven.abstractions.basic.Lazy;
import raven.client.IDocumentQuery;

import com.mysema.query.types.Expression;
import com.mysema.query.types.OrderSpecifier;
import com.mysema.query.types.Predicate;

//TODO: insert linq expressions  + RavenQueryableExtensions + LinqExntesions
public interface IOrderedQueryable<T> {
  /**
   * Filters a sequence of values based on a predicate.
   * @param predicate
   * @return
   */
  public IRavenQueryable<T> where(Predicate predicate);

  /**
   * Changes order of result elements
   * @param asc
   * @return
   */
  public IRavenQueryable<T> orderBy(OrderSpecifier<?>... asc);

  //TODO: finish me

  public List<T> toList();

  public T single();

  public Lazy<List<T>> lazily();


  //IQueryable

  public Class<?> getElementType();

  public Expression<?> getExpression();

  public IQueryProvider getProvider();





}
