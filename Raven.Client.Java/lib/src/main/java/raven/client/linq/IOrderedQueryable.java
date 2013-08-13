package raven.client.linq;

import java.util.List;

import raven.abstractions.basic.Lazy;

import com.mysema.query.types.Predicate;

//TODO: insert linq expressions  + RavenQueryableExtensions + LinqExntesions
public interface IOrderedQueryable<T> {
  /**
   * Filters a sequence of values based on a predicate.
   * @param predicate
   * @return
   */
  public IRavenQueryable<T> where(Predicate predicate);

  //TODO: finish me

  public List<T> toList();


  public Lazy<List<T>> lazily();
}
