package raven.linq.dsl;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.mysema.query.types.Expression;
import com.mysema.query.types.OrderSpecifier;
import com.mysema.query.types.Path;
import com.mysema.query.types.Predicate;
import com.mysema.query.types.path.EntityPathBase;
import com.mysema.query.types.path.ListPath;

import raven.client.util.Inflector;
import raven.linq.dsl.expressions.AnonymousExpression;

/**
 * Represents Index definition
 */
//TODO: this is also used in transformers - consider different name!
@Deprecated
public class IndexExpression {
  public static IndexExpression from(Class<?> objectClass) {
    return new IndexExpression(LinqExpressionMixin.DOCS_ROOT_NAME + "." + Inflector.pluralize(objectClass.getSimpleName()));
  }

  public static IndexExpression whereEntityIs(String... subClasses) {
    List<String> plurar = new ArrayList<>();
    for (String subClass: subClasses) {
      plurar.add("\"" + Inflector.pluralize(subClass) + "\"");
    }
    String subClassesJoined = StringUtils.join(plurar, ", ");

    return new IndexExpression(LinqExpressionMixin.DOCS_ROOT_NAME + ".WhereEntityIs(new string[] { " + subClassesJoined + " })");
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

  public IndexExpression select(Expression< ? > projection) {
    return expressionMixin.select(projection);
  }

  public <S> IndexExpression selectMany(ListPath<S, ? extends EntityPathBase<S>> selector, Path<?> nestedRoot) {
    return expressionMixin.selectMany(selector, nestedRoot);
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

  public IndexExpression selectMany(Expression<?> selector, AnonymousExpression anonymousClass) {
    return expressionMixin.selectMany(selector, anonymousClass);
  }


}
