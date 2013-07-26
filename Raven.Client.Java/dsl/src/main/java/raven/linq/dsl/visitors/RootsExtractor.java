package raven.linq.dsl.visitors;

import java.util.List;

import raven.linq.dsl.LinqOps;

import com.mysema.query.types.Constant;
import com.mysema.query.types.Expression;
import com.mysema.query.types.FactoryExpression;
import com.mysema.query.types.Operation;
import com.mysema.query.types.ParamExpression;
import com.mysema.query.types.Path;
import com.mysema.query.types.SubQueryExpression;
import com.mysema.query.types.TemplateExpression;
import com.mysema.query.types.Visitor;

/**
 * Extracts all path roots
 */
public class RootsExtractor implements Visitor<Void, RootsExtractorContext> {

  public static final RootsExtractor DEFAULT = new RootsExtractor();

  @Override
  public Void visit(Constant< ? > expr, RootsExtractorContext context) {
    return null;
  }

  @Override
  public Void visit(FactoryExpression< ? > expr, RootsExtractorContext context) {
    for (Expression<?> e : expr.getArgs()) {
        e.accept(this, context);
    }
    return null;
  }

  private Void visit(List<Expression<?>> exprs, RootsExtractorContext context) {
    for (Object e : exprs) {
      ((Expression<?>)e).accept(this, context);
    }
    return null;
  }

  @Override
  public Void visit(Operation< ? > expr, RootsExtractorContext context) {
    if (LinqOps.LAMBDA.equals(expr.getOperator())) {
      Expression< ? > lambdaLeft = expr.getArg(0);
      Expression<?> lambdaRight = expr.getArg(1);
      if (lambdaLeft instanceof Path<?>) {
        Path<?> leftPath = (Path<?>) lambdaLeft;
        String leftPathName = leftPath.getMetadata().getName();
        context.addLambda(leftPathName);
        lambdaRight.accept(this, context);
        context.getIntroducedInLambda().remove(leftPathName);
      } else {
        throw new IllegalStateException("Unexpected expression in lambda left:" + expr);
      }

    } else if (LinqOps.SUM.equals(expr.getOperator())) {
      // do nothing
    } else {
      visit(expr.getArgs(), context);
    }
    return null;
  }

  @Override
  public Void visit(ParamExpression< ? > expr, RootsExtractorContext context) {
    return null;
  }

  @Override
  public Void visit(Path< ? > expr, RootsExtractorContext context) {
    String rootName = expr.getRoot().getMetadata().getName();
    if (!context.getIntroducedInLambda().contains(rootName)) {
      context.addRoot(rootName);
    }
    return null;
  }

  @Override
  public Void visit(SubQueryExpression< ? > expr, RootsExtractorContext context) {
    // we don't descent to subqueries
    return null;
  }

  @Override
  public Void visit(TemplateExpression< ? > expr, RootsExtractorContext context) {
    // we don't parse template expressions
    return null;
  }


}
