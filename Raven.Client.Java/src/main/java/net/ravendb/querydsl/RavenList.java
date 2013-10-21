package net.ravendb.querydsl;

import net.ravendb.abstractions.LinqOps;

import com.mysema.query.types.Path;
import com.mysema.query.types.PathMetadata;
import com.mysema.query.types.expr.BooleanExpression;
import com.mysema.query.types.expr.BooleanOperation;
import com.mysema.query.types.expr.SimpleExpression;
import com.mysema.query.types.path.ListPath;
import com.mysema.query.types.path.PathInits;

/**
 * Extends QueryDSL ListPath to use binary ANY instead of unary.
 * @param <E>
 * @param <Q>
 */
public class RavenList<E, Q extends SimpleExpression<? super E>> extends ListPath<E, Q> {

  public RavenList(Class< ? super E> elementType, Class<Q> queryType, Path< ? > parent, String property) {
    super(elementType, queryType, parent, property);
  }

  public RavenList(Class< ? super E> elementType, Class<Q> queryType, PathMetadata< ? > metadata, PathInits inits) {
    super(elementType, queryType, metadata, inits);
  }

  public RavenList(Class< ? super E> elementType, Class<Q> queryType, PathMetadata< ? > metadata) {
    super(elementType, queryType, metadata);
  }

  public RavenList(Class< ? super E> elementType, Class<Q> queryType, String variable) {
    super(elementType, queryType, variable);
  }

  /**
   * This method is deprecated. Use any(BooleanExpression) instead.
   */
  @Override
  @Deprecated
  public Q any() {
    throw new IllegalStateException("This method is deprecated. Use any(BooleanExpression) instead.");
  }

  public BooleanExpression any(BooleanExpression boolExpr) {
    return BooleanOperation.create(LinqOps.Query.ANY, mixin, boolExpr);
  }

  public Q select() {
    return get(0);
  }

}
