package net.ravendb.client.linq;

import com.mysema.query.types.Expression;

public interface IQueryProvider {
  public <T> IRavenQueryable<T> createQuery(Expression<?> expression);

  public Object execute(Expression<?> expression);
}
