package raven.linq.dsl.visitors;

import java.util.List;
import java.util.Set;

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
public class RootsExtractor implements Visitor<Void, Set<String>> {

  public static final RootsExtractor DEFAULT = new RootsExtractor();

  @Override
  public Void visit(Constant< ? > expr, Set<String> context) {
    return null;
  }

  @Override
  public Void visit(FactoryExpression< ? > expr, Set<String> context) {
    for (Expression<?> e : expr.getArgs()) {
        e.accept(this, context);
    }
    return null;
  }

  private Void visit(List<Expression<?>> exprs, Set<String> context) {
    for (Object e : exprs) {
      ((Expression<?>)e).accept(this, context);
    }
    return null;
  }

  @Override
  public Void visit(Operation< ? > expr, Set<String> context) {
    if (LinqOps.SUM.equals(expr.getOperator())) {
      // do nothing
    } else {
      visit(expr.getArgs(), context);
    }
    return null;
  }

  @Override
  public Void visit(ParamExpression< ? > expr, Set<String> context) {
    return null;
  }

  @Override
  public Void visit(Path< ? > expr, Set<String> context) {
    context.add(expr.getRoot().getMetadata().getName());
    return null;
  }

  @Override
  public Void visit(SubQueryExpression< ? > expr, Set<String> context) {
    // we don't descent to subqueries
    return null;
  }

  @Override
  public Void visit(TemplateExpression< ? > expr, Set<String> context) {
    // we don't parse template expressions
    return null;
  }


}
