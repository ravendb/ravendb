package raven.querydsl;

import raven.abstractions.LinqOps;

import com.google.common.collect.ImmutableList;
import com.mysema.query.support.Expressions;
import com.mysema.query.types.Constant;
import com.mysema.query.types.Expression;
import com.mysema.query.types.FactoryExpression;
import com.mysema.query.types.Operation;
import com.mysema.query.types.OperationImpl;
import com.mysema.query.types.Operator;
import com.mysema.query.types.ParamExpression;
import com.mysema.query.types.Path;
import com.mysema.query.types.PathType;
import com.mysema.query.types.Predicate;
import com.mysema.query.types.PredicateOperation;
import com.mysema.query.types.SubQueryExpression;
import com.mysema.query.types.TemplateExpression;
import com.mysema.query.types.TemplateExpressionImpl;
import com.mysema.query.types.Templates;
import com.mysema.query.types.Visitor;
import com.mysema.query.types.expr.Param;
import com.mysema.query.types.template.BooleanTemplate;

public class CollectionAnyVisitor implements Visitor<Expression<?>,StackBasedContext> {

  public static final Templates TEMPLATES = new Templates() {
    {
      add(PathType.PROPERTY, "{0}_{1}");
      add(PathType.COLLECTION_ANY, "{0}");
    }};


    @Override
    public Expression<?> visit(Constant<?> expr, StackBasedContext context) {
      return expr;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public Expression<?> visit(TemplateExpression<?> expr, StackBasedContext context) {
      try {
        context.push(expr);
        Object[] args = new Object[expr.getArgs().size()];
        for (int i = 0; i < args.length; i++) {
          StackBasedContext c = new StackBasedContext(context);
          if (expr.getArg(i) instanceof Expression) {
            args[i] = ((Expression)expr.getArg(i)).accept(this, c);
          } else {
            args[i] = expr.getArg(i);
          }
          context.mergeReplace(c);
        }
        if (context.isReplace()) {
          if (expr.getType().equals(Boolean.class)) {
            Predicate predicate = BooleanTemplate.create(expr.getTemplate(), args);
            return predicate;
          } else {
            return TemplateExpressionImpl.create(expr.getType(), expr.getTemplate(), args);
          }
        } else {
          return expr;
        }
      } finally {
        context.pop();
      }
    }

    @Override
    public Expression<?> visit(FactoryExpression<?> expr, StackBasedContext context) {
      context.push(expr);

      context.pop();
      return expr;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public Expression<?> visit(Operation<?> expr, StackBasedContext context) {
      try {
        context.push(expr);
        Expression<?>[] args = new Expression<?>[expr.getArgs().size()];
        for (int i = 0; i < args.length; i++) {
          StackBasedContext c = new StackBasedContext(context);
          args[i] = expr.getArg(i).accept(this, c);
          context.mergeReplace(c);
        }
        if (context.isReplace()) {
          if (args.length > 0 && args[0] instanceof Operation) {
            Operation<?> someOperation = (Operation< ? >) args[0];
            if (someOperation.getOperator().equals(LinqOps.Query.ANY)) {
              // ignoring second arg as it was moved under ANY node
              return someOperation;
            }
          }
          if (expr.getType().equals(Boolean.class)) {
            return new PredicateOperation((Operator)expr.getOperator(), ImmutableList.copyOf(args));
          } else {
            return new OperationImpl(expr.getType(), (Operator)expr.getOperator(), ImmutableList.copyOf(args));
          }
        } else {
          return expr;
        }
      } finally {
        context.pop();

      }
    }


    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public Expression<?> visit(Path<?> expr, StackBasedContext context) {
      try {
        context.push(expr);
        if (expr.getMetadata().getPathType() == PathType.COLLECTION_ANY) {
          Path<?> parent = (Path<?>) expr.getMetadata().getParent().accept(this, context);

          Operation< ? > parentOperation = (Operation< ? >) context.getExpressionStack().get(context.getExpressionStack().size() - 2);
          // left node of parent expression is our Any expression
          Expression< ? > opRight = parentOperation.getArg(1);
          Expression< ? > opLeft = new Param<>(Object.class, "r");

          Operation<?> newOp = (Operation< ? >) Expressions.operation(parentOperation.getType(),(Operator) parentOperation.getOperator(), opLeft, opRight);
          Operation<?> anyOp = (Operation< ? >) Expressions.operation(Object.class, LinqOps.Query.ANY, parent, newOp);
          context.setReplace(true);
          return anyOp;
        }
        return expr;
      } finally {
        context.pop();

      }
    }

    @Override
    public Expression<?> visit(SubQueryExpression<?> expr, StackBasedContext context) {
      return expr;
    }

    @Override
    public Expression<?> visit(ParamExpression<?> expr, StackBasedContext context) {
      return expr;
    }
}
