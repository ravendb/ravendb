package raven.linq.dsl;

import com.mysema.query.types.Predicate;

import raven.client.util.Inflector;

/**
 * Represents LinqQuery
 */
public class LinqQuery {

  public static LinqQuery from(Class<?> objectClass) {
    return new LinqQuery(LinqExpressionMixin.DOCS_ROOT_NAME + "." + Inflector.pluralize(objectClass.getSimpleName()));
  }

  public static LinqQuery from(String customRoot) {
    return new LinqQuery(customRoot);
  }


  private LinqExpressionMixin<LinqQuery> expressionMixin;
  protected LinqQuery(String rootName) {
    expressionMixin = new LinqExpressionMixin<LinqQuery>(this, rootName);
  }

  public String toLinq() {
    return expressionMixin.toLinq();
  }

  public String toString() {
    return expressionMixin.toString();
  }

  public LinqQuery where(Predicate e) {
    return expressionMixin.where(e);
  }

}
