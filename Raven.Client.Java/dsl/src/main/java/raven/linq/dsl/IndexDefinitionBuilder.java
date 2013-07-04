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
public class IndexDefinitionBuilder {
  public static IndexDefinitionBuilder from(Class<?> objectClass) {
    return new IndexDefinitionBuilder(LinqExpressionMixin.DOCS_ROOT_NAME + "." + Inflector.pluralize(objectClass.getSimpleName()));
  }


  public static IndexDefinitionBuilder from(String customRoot) {
    return new IndexDefinitionBuilder(customRoot);
  }

  private LinqExpressionMixin<IndexDefinitionBuilder> expressionMixin;
  protected IndexDefinitionBuilder(String rootName) {
    expressionMixin = new LinqExpressionMixin<IndexDefinitionBuilder>(this, rootName);
  }

  public IndexDefinitionBuilder groupBy(Expression< ? > keySelector) {
    return expressionMixin.groupBy(keySelector);
  }

  public IndexDefinitionBuilder orderBy(OrderSpecifier< ? >... orderSpecifiers) {
    return expressionMixin.orderBy(orderSpecifiers);
  }

  public IndexDefinitionBuilder select(FactoryExpression< ? > projection) {
    return expressionMixin.select(projection);
  }

  public <S> IndexDefinitionBuilder selectMany(ListPath<S, ? extends EntityPathBase<S>> selector) {
    return expressionMixin.selectMany(selector);
  }

  public String toLinq() {
    return expressionMixin.toLinq();
  }

  public String toString() {
    return expressionMixin.toString();
  }

  public IndexDefinitionBuilder where(Predicate e) {
    return expressionMixin.where(e);
  }


}
