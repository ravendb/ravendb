package raven.linq.dsl;

import com.mysema.query.support.Expressions;
import com.mysema.query.types.Expression;
import com.mysema.query.types.OperationImpl;
import com.mysema.query.types.Operator;
import com.mysema.query.types.Order;
import com.mysema.query.types.OrderSpecifier;
import com.mysema.query.types.Predicate;
import com.mysema.query.types.path.EntityPathBase;
import com.mysema.query.types.path.ListPath;

public final class LinqExpressionMixin<T> implements Cloneable {

  public final static String DOCS_ROOT_NAME = "docs";

  private LambdaInferer lambdaInferer;

  private Expression<?> expression;

  private T self;

  protected LinqExpressionMixin(T self, String rootName) {
    expression = Expressions.template(LinqExpressionMixin.class, rootName);
    lambdaInferer = LambdaInferer.DEFAULT;
    this.self = self;
  }


  public Expression< ? > getExpression() {
    return expression;
  }

  public T groupBy(Expression<?> keySelector) {
    expression = OperationImpl.create(LinqExpressionMixin.class, LinqOps.Fluent.GROUP_BY, expression, lambdaInferer.inferLambdas(keySelector));
    return self;
  }

  @SuppressWarnings("rawtypes")
  public T orderBy(OrderSpecifier< ? >... orderSpecifiers) {
    for (OrderSpecifier< ? > order : orderSpecifiers) {
      Operator<LinqExpressionMixin> operator = (order.getOrder().equals(Order.ASC)) ? LinqOps.Fluent.ORDER_BY : LinqOps.Fluent.ORDER_BY_DESC;
      expression = OperationImpl.create(LinqExpressionMixin.class, operator, expression, lambdaInferer.inferLambdas(order.getTarget()));
    }
    return self;
  }

  public T select(Expression<?> projection) {
    expression = OperationImpl.create(LinqExpressionMixin.class, LinqOps.Fluent.SELECT, expression, lambdaInferer.inferLambdas(projection));
    return self;
  }

  public <S> T selectMany(ListPath<S, ? extends EntityPathBase<S>> selector) {
    expression = OperationImpl.create(LinqExpressionMixin.class, LinqOps.Fluent.SELECT_MANY, expression, lambdaInferer.inferLambdas(selector));
    return self;
  }

  public String toLinq() {
    return new LinqSerializer(LinqQueryTemplates.DEFAULT).toLinq(this);
  }

  @Override
  public String toString() {
    return toLinq();
  }

  public T where(Predicate e) {
    expression = OperationImpl.create(LinqExpressionMixin.class, LinqOps.Fluent.WHERE, expression, lambdaInferer.inferLambdas(e));
    return self;
  }

}
