package raven.linq.dsl.expressions;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.google.common.collect.ImmutableList;
import com.mysema.query.support.Expressions;
import com.mysema.query.types.Expression;
import com.mysema.query.types.ExpressionBase;
import com.mysema.query.types.FactoryExpression;
import com.mysema.query.types.Ops;
import com.mysema.query.types.Path;
import com.mysema.query.types.Visitor;
public class AnonymousExpression extends ExpressionBase<Object> implements FactoryExpression<Object> {

  public static AnonymousExpression create() {
    return new AnonymousExpression();
  }

  private List<Expression<?>> expressions = new ArrayList<>();

  public AnonymousExpression() {
    super(Object.class);
  }

  public AnonymousExpression(Expression<?>...args) {
    super(Object.class);
    this.expressions = ImmutableList.copyOf(args);
  }

  @Override
  public <R, C> R accept(Visitor<R, C> v, C context) {
    return v.visit(this, context);
  }

  protected String extractPropName(Path<?> path) {
    return StringUtils.capitalize(path.getMetadata().getName());
  }

  @Override
  public List<Expression< ? >> getArgs() {
    return expressions;
  }

  public int getParamsCount() {
    return expressions.size();
  }

  @Override
  public Object newInstance(Object... args) {
    throw new IllegalStateException("this method is not implemented");
  }

  public AnonymousExpression withTemplate(String propertyName, String templateExpression) {
    return with(propertyName, Expressions.template(Object.class, templateExpression));
  }

  public <S> AnonymousExpression withTemplate(Path<? extends S> path, String templateExpression) {
    return with(extractPropName(path), Expressions.template(Object.class, templateExpression));
  }

  public <S> AnonymousExpression with(Path<? extends S> path, Expression<? extends S> selector) {
    return with(extractPropName(path), selector);
  }

  public <S> AnonymousExpression with(Path<S> path, S constant) {
    return with(path, Expressions.constant(constant));
  }

  public AnonymousExpression with(Path<?> path) {
    expressions.add(Expressions.operation(List.class, Ops.LIST, path));
    return this;
  }

  public AnonymousExpression with(String propertyName, Expression<?> selector) {
    if (propertyName.contains(".")) {
      throw new RuntimeException("propertyName can not contain nested paths!");
    }
    expressions.add(Expressions.operation(List.class, Ops.LIST, Expressions.constant(propertyName), selector));

    return this;
  }

  public <S> AnonymousExpression with(String propertyName, Object constant) {
    return with(propertyName, Expressions.constant(constant));
  }

}
