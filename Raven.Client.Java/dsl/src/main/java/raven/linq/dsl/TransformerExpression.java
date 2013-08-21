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
public class TransformerExpression {
  public static TransformerExpression from(Class<?> objectClass) {
    return new TransformerExpression(LinqExpressionMixin.DOCS_ROOT_NAME + "." + Inflector.pluralize(objectClass.getSimpleName()));
  }

  public static TransformerExpression whereEntityIs(String... subClasses) {
    List<String> plurar = new ArrayList<>();
    for (String subClass: subClasses) {
      plurar.add("\"" + Inflector.pluralize(subClass) + "\"");
    }
    String subClassesJoined = StringUtils.join(plurar, ", ");

    return new TransformerExpression(LinqExpressionMixin.DOCS_ROOT_NAME + ".WhereEntityIs(new string[] { " + subClassesJoined + " })");
  }

  public static TransformerExpression from(String customRoot) {
    return new TransformerExpression(customRoot);
  }

  private LinqExpressionMixin<TransformerExpression> expressionMixin;
  protected TransformerExpression(String rootName) {
    expressionMixin = new LinqExpressionMixin<TransformerExpression>(this, rootName);
  }

  public TransformerExpression groupBy(Expression< ? > keySelector) {
    return expressionMixin.groupBy(keySelector);
  }



  public TransformerExpression orderBy(OrderSpecifier< ? >... orderSpecifiers) {
    return expressionMixin.orderBy(orderSpecifiers);
  }

  public TransformerExpression select(Expression< ? > projection) {
    return expressionMixin.select(projection);
  }

  public <S> TransformerExpression selectMany(ListPath<S, ? extends EntityPathBase<S>> selector, Path<?> nestedRoot) {
    return expressionMixin.selectMany(selector, nestedRoot);
  }

  public String toLinq() {
    return expressionMixin.toLinq();
  }

  public String toString() {
    return expressionMixin.toString();
  }

  public TransformerExpression where(Predicate e) {
    return expressionMixin.where(e);
  }

  public TransformerExpression selectMany(Expression<?> selector, AnonymousExpression anonymousClass) {
    return expressionMixin.selectMany(selector, anonymousClass);
  }


}
