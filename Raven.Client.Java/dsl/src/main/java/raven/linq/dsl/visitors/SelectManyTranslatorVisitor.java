package raven.linq.dsl.visitors;

import org.apache.commons.lang.StringUtils;

import raven.linq.dsl.LinqExpressionMixin;
import raven.linq.dsl.LinqOps;
import raven.linq.dsl.LinqQueryTemplates;
import raven.linq.dsl.expressions.AnonymousExpression;

import com.google.common.collect.ImmutableList;
import com.mysema.query.support.Expressions;
import com.mysema.query.types.Constant;
import com.mysema.query.types.Expression;
import com.mysema.query.types.FactoryExpression;
import com.mysema.query.types.Operation;
import com.mysema.query.types.OperationImpl;
import com.mysema.query.types.Operator;
import com.mysema.query.types.Ops;
import com.mysema.query.types.ParamExpression;
import com.mysema.query.types.Path;
import com.mysema.query.types.PredicateOperation;
import com.mysema.query.types.SubQueryExpression;
import com.mysema.query.types.TemplateExpression;
import com.mysema.query.types.ToStringVisitor;
import com.mysema.query.types.Visitor;
import com.mysema.query.types.expr.SimpleExpression;
import com.mysema.query.types.path.PathBuilder;
/**
 * This visitor is responsible for AST translation. During SelectMany we have to introduce anonymous classes and refer parent
 * object to properly return with index entries.
 */
public class SelectManyTranslatorVisitor implements Visitor<Expression<?>, LocationAwareContext> {

  public final static String TRANSIENT_ID_PREFIX = "transId_";

  @Override
  public Expression<?> visit(Constant< ? > expr, LocationAwareContext context) {
    return expr;
  }

  @Override
  public Expression<?> visit(FactoryExpression< ? > expr, LocationAwareContext context) {
    Expression<?>[] args = new Expression<?>[expr.getArgs().size()];

    for (int i = 0; i < args.length; i++) {
      args[i] = expr.getArgs().get(i).accept(this, context);
    }
    if (context.replace && expr instanceof AnonymousExpression<?>) {
      return new AnonymousExpression<>(expr.getType(), args);
    } else {
      return expr;
    }
  }

  @Override
  public Expression<?> visit(Operation< ? > expr, LocationAwareContext context) {
    Expression<?>[] args = new Expression<?>[expr.getArgs().size()];

    for (int i = 0; i < args.length; i++) {
      args[i] = expr.getArg(i).accept(this, context);
    }

    if (LinqOps.LAMBDA.equals(expr.getOperator())) {
      Path<?> variablePath = (Path< ? >) args[0];
      Path< ? > root = variablePath.getRoot();
      if (root != variablePath) {
        context.replace = true;
        return replaceCurrentOperation(expr, new Expression[] { root, args[1] });
      }

    } else if (LinqOps.Fluent.SELECT_MANY.equals(expr.getOperator())) {
      return handleSelectMany(context, args);
    }
    if (context.replace) {
      return replaceCurrentOperation(expr, args);
    } else {
      return expr;
    }

  }
  @SuppressWarnings({ "unchecked", "rawtypes" })
  private Expression< ? > replaceCurrentOperation(Operation< ? > originalExpression, Expression< ? >[] newArgs) {
    if (originalExpression.getType().equals(Boolean.class)) {
      return new PredicateOperation((Operator)originalExpression.getOperator(), ImmutableList.copyOf(newArgs));
    } else {
      return new OperationImpl(originalExpression.getType(), (Operator)originalExpression.getOperator(), ImmutableList.copyOf(newArgs));
    }
  }

  /**
   * <pre>
   * -root
   *    +-- AST before SelectMany invocation
   *    + -- Op: LIST  (rightOp)
   *         +-- Op: LAMBDA (leftLambda)
   *         |   +-- Path: variable name (variableName)
   *         |   +-- Expr: expression
   *         +-- Path: alias for nested (aliasForNested)
   * </pre>
   * @param originalExpression
   * @param context
   * @param visitedArgs
   * @return
   */
  @SuppressWarnings("rawtypes")
  private Expression< ? > handleSelectMany(LocationAwareContext context, Expression< ? >[] visitedArgs) {

    // 1. let's start with extracting nodes from our AST tree...
    Operation<?> rightOp =  (Operation< ? >) visitedArgs[1];
    if (!Ops.LIST.equals(rightOp.getOperator())) {
      throw new IllegalStateException("Expected LIST operation in right node of SELECT_MANY. Got: " + rightOp.getOperator().getId());
    }
    Operation< ? > leftLambda = (Operation< ? >) rightOp.getArg(0);
    if (!LinqOps.LAMBDA.equals(leftLambda.getOperator())) {
      throw new IllegalStateException("Expected LAMBDA operation. Got: " + leftLambda.getOperator().getId());
    }

    AnonymousExpression<?> selectManySelector = null;
    Expression<?> wrappedParamsList = null;

    Path< ? > variableName = (Path< ? >) leftLambda.getArg(0);
    Path< ? > aliasForNested = null;
    if (rightOp.getArg(1) instanceof Operation<?>) {
      Operation<?> lambdaOp =  (Operation< ? >) rightOp.getArg(1);
      aliasForNested = (Path< ? >) lambdaOp.getArg(0);
      selectManySelector = (AnonymousExpression< ? >) lambdaOp.getArg(1);
    } else {
      aliasForNested = (Path< ? >) rightOp.getArg(1);
      // Create expression body for second argument for SelectMany called selector function
      selectManySelector = AnonymousExpression
          .create(Object.class)
          .with(StringUtils.capitalize(variableName.getMetadata().getName()), variableName)
          .with(StringUtils.capitalize(aliasForNested.getMetadata().getName()), aliasForNested);

   // Register paths for rename
      String leftName = variableName.accept(ToStringVisitor.DEFAULT, LinqQueryTemplates.DEFAULT).replace('.', '_');
      String rightName = aliasForNested.accept(ToStringVisitor.DEFAULT, LinqQueryTemplates.DEFAULT).replace('.', '_');
      PathBuilder<?> transientEntityPath = new PathBuilder<>(AnonymousExpression.class, TRANSIENT_ID_PREFIX + context.innerExprNum);
      context.innerExprNum++;
      context.add(variableName, transientEntityPath.get(leftName));
      context.add(aliasForNested, transientEntityPath.get(rightName));
    }

    // Create variables for selector function. Produces: (leftAlias, aliasForNested)
    wrappedParamsList = Expressions.operation(LinqExpressionMixin.class, Ops.WRAPPED,
        Expressions.operation(LinqExpressionMixin.class, Ops.LIST, variableName.getRoot(), aliasForNested));


    // Create new lambda expression: leave root only from variable name (could be renamed due to replace algorithm), leave right node as it was)
    SimpleExpression<?> newLeftLambda = Expressions.operation(LinqExpressionMixin.class, LinqOps.LAMBDA, variableName.getRoot(), leftLambda.getArg(1));

    // And combine left and right node into LAMBDA expression
    SimpleExpression<LinqExpressionMixin> selectManySelectorLambda = Expressions.operation(LinqExpressionMixin.class, LinqOps.LAMBDA, wrappedParamsList, selectManySelector);


    return Expressions.operation(LinqExpressionMixin.class, LinqOps.Fluent.SELECT_MANY_TRANSLATED, visitedArgs[0], newLeftLambda, selectManySelectorLambda);
  }

  @Override
  public Expression<?> visit(ParamExpression< ? > expr, LocationAwareContext context) {
    return expr;
  }

  @Override
  public Expression<?> visit(Path< ? > expr, LocationAwareContext context) {
    // check if we can replace parent node

    if (expr.getMetadata().getParent() != null) {
      Path<?> newParent = (Path< ? >) expr.getMetadata().getParent().accept(this, context);
      if (newParent != expr.getMetadata().getParent()) {
        // parent was changed!
        return Expressions.path(expr.getType(), newParent, expr.getMetadata().getName());
      }
    }
    // try to find replacement for current path

    for (int i = 0; i < context.paths.size(); i++) {
      if (context.paths.get(i).equals(expr)) {
        return context.replacements.get(i);
      }
    }
    return expr;
  }

  @Override
  public Expression<?> visit(SubQueryExpression< ? > expr, LocationAwareContext context) {
    return expr;
  }

  @Override
  public Expression<?> visit(TemplateExpression< ? > expr, LocationAwareContext context) {
    return expr;
  }

}
