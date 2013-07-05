package raven.linq.dsl;

import com.mysema.query.types.Expression;
import com.mysema.query.types.FactoryExpression;
import com.mysema.query.types.OrderSpecifier;
import com.mysema.query.types.Predicate;
import com.mysema.query.types.path.EntityPathBase;
import com.mysema.query.types.path.ListPath;

import raven.client.util.Inflector;

/**
 * Represents Index definition
 */
public class IndexExpression {
  public static IndexExpression from(Class<?> objectClass) {
    return new IndexExpression(LinqExpressionMixin.DOCS_ROOT_NAME + "." + Inflector.pluralize(objectClass.getSimpleName()));
  }


  public static IndexExpression from(String customRoot) {
    return new IndexExpression(customRoot);
  }

  private LinqExpressionMixin<IndexExpression> expressionMixin;
  protected IndexExpression(String rootName) {
    expressionMixin = new LinqExpressionMixin<IndexExpression>(this, rootName);
  }

  public IndexExpression groupBy(Expression< ? > keySelector) {
    return expressionMixin.groupBy(keySelector);
  }

  public IndexExpression orderBy(OrderSpecifier< ? >... orderSpecifiers) {
    return expressionMixin.orderBy(orderSpecifiers);
  }

  public IndexExpression select(FactoryExpression< ? > projection) {
    return expressionMixin.select(projection);
  }

  public <S> IndexExpression selectMany(ListPath<S, ? extends EntityPathBase<S>> selector) {
    return expressionMixin.selectMany(selector);
  }

  public String toLinq() {
    return expressionMixin.toLinq();
  }

  public String toString() {
    return expressionMixin.toString();
  }

  public IndexExpression where(Predicate e) {
    return expressionMixin.where(e);
  }


}
