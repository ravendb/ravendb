package net.ravendb.abstractions.extensions;

import java.util.Stack;

import org.apache.commons.lang.StringUtils;

import com.mysema.query.types.Constant;
import com.mysema.query.types.Expression;
import com.mysema.query.types.FactoryExpression;
import com.mysema.query.types.Operation;
import com.mysema.query.types.ParamExpression;
import com.mysema.query.types.Path;
import com.mysema.query.types.PathType;
import com.mysema.query.types.SubQueryExpression;
import com.mysema.query.types.TemplateExpression;
import com.mysema.query.types.Visitor;

public class ExpressionExtensions {

  public static String toPropertyPath(Expression<?> expr) {
    return toPropertyPath(expr, '.', ',');
  }

  public static String toPropertyPath(Expression<?> expr, char propertySeparator) {
    return ExpressionExtensions.toPropertyPath(expr, propertySeparator, ',');
  }

  public static String toPropertyPath(Expression<?> expr, char propertySeparator, char collectionSeparator) {
    Stack<String> results = new Stack<>();
    expr.accept(new PropertyPathExpressionVisitor(String.valueOf(propertySeparator), String.valueOf(collectionSeparator)), results);
    StringBuilder sb = new StringBuilder();
    while (!results.isEmpty()) {
      sb.append(results.pop());
    }
    return StringUtils.stripEnd(sb.toString(), String.valueOf(propertySeparator) + String.valueOf(collectionSeparator));
  }

  private static class PropertyPathExpressionVisitor implements Visitor<Void, Stack<String>> {
    private String propertySeparator;
    private String collectionSeparator;

    public PropertyPathExpressionVisitor(String propertySeparator, String collectionSeparator) {
      super();
      this.propertySeparator = propertySeparator;
      this.collectionSeparator = collectionSeparator;
    }

    @Override
    public Void visit(Constant< ? > expr, Stack<String> context) {
      throw new IllegalStateException("Detected " + expr.getType() + " in path expression!");
    }

    @Override
    public Void visit(FactoryExpression< ? > expr, Stack<String> context) {
      throw new IllegalStateException("Detected " + expr.getType() + " in path expression!");
    }

    @Override
    public Void visit(Operation< ? > expr, Stack<String> context) {
      throw new IllegalStateException("Detected " + expr.getType() + " in path expression!");
    }

    @Override
    public Void visit(ParamExpression< ? > expr, Stack<String> context) {
      throw new IllegalStateException("Detected " + expr.getType() + " in path expression!");
    }

    @Override
    public Void visit(Path< ? > expr, Stack<String> context) {
      Path< ? > parent = expr.getMetadata().getParent();
      if (parent != null) {

        if (expr.getMetadata().getPathType() == PathType.LISTVALUE_CONSTANT) {
          context.push(collectionSeparator);
          parent.accept(this, context);
        } else {
          context.push(propertySeparator);
          context.push(StringUtils.capitalize(expr.getMetadata().getName()));
          parent.accept(this, context);
        }
      }
      return null;
    }

    @Override
    public Void visit(SubQueryExpression< ? > expr, Stack<String> context) {
      throw new IllegalStateException("Detected " + expr.getType() + " in path expression!");
    }

    @Override
    public Void visit(TemplateExpression< ? > expr, Stack<String> context) {
      throw new IllegalStateException("Detected " + expr.getType() + " in path expression!");
    }

  }
}
