package net.ravendb.querydsl;

import net.ravendb.abstractions.LinqOps;

import com.mysema.query.types.ConstantImpl;
import com.mysema.query.types.Expression;
import com.mysema.query.types.Path;
import com.mysema.query.types.PathMetadata;
import com.mysema.query.types.expr.BooleanExpression;
import com.mysema.query.types.expr.BooleanOperation;
import com.mysema.query.types.path.StringPath;

/**
 * Custom extensions for build-in StringPath
 */
public class RavenString extends StringPath {

  public RavenString(Path< ? > parent, String property) {
    super(parent, property);
  }

  public RavenString(PathMetadata< ? > metadata) {
    super(metadata);
  }

  public RavenString(String var) {
    super(var);
  }

  /**
   * Compares this {@code StringExpression} to another {@code StringExpression}, NOT ignoring case
   * considerations.
   *
   * @param str
   * @return this.equalsNotIgnoreCase(str)
   * @see java.lang.String#equalsIgnoreCase(String)
   */
  public BooleanExpression equalsNotIgnoreCase(Expression<String> str) {
      return BooleanOperation.create(LinqOps.Ops.EQ_NOT_IGNORE_CASE, mixin, str);
  }

  /**
   * Compares this {@code StringExpression} to another {@code StringExpression}, NOT ignoring case
   * considerations.
   *
   * @param str
   * @return this.equalsNotIgnoreCase(str)
   * @see java.lang.String#equalsIgnoreCase(String)
   */
  public BooleanExpression equalsNotIgnoreCase(String str) {
      return equalsNotIgnoreCase(ConstantImpl.create(str));
  }


}
