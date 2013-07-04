package raven.linq.dsl;

import java.util.HashSet;
import java.util.Set;

import raven.linq.dsl.visitors.RootsExtractor;

import com.mysema.query.support.Expressions;
import com.mysema.query.types.Expression;
import com.mysema.query.types.OperationImpl;
import com.mysema.query.types.TemplateExpression;
import com.mysema.query.types.path.SimplePath;

/**
 *  Infers roots from expression and decorate with lambdas
 *  If expression is template return with out modification
 */
public class LambdaInferer {
  public final static LambdaInferer DEFAULT = new LambdaInferer();


  private Set<String> extractRoots(Expression<?> expr) {
    Set<String> context = new HashSet<>();
    expr.accept(RootsExtractor.DEFAULT, context);
    return context;
  }


  public Expression<?> inferLambdas(Expression<?> expr) {
    if (expr instanceof TemplateExpression) {
      return expr;
    }

    Set<String> extractRoots = extractRoots(expr);
    if (extractRoots.size() != 1) {
      throw new RuntimeException("Can not find single expression root: " + extractRoots);
    }
    String root = extractRoots.iterator().next();
    SimplePath<Object> rootPath = Expressions.path(Object.class, root);

    return OperationImpl.create(LinqExpressionMixin.class, LinqOps.LAMBDA, rootPath, expr);
  }


}
