package raven.linq.dsl;


import org.apache.commons.lang.StringUtils;

import raven.linq.dsl.expressions.AnonymousExpression;
import raven.linq.dsl.utils.PathUtils;

import com.mysema.query.support.SerializerBase;
import com.mysema.query.types.Constant;
import com.mysema.query.types.Expression;
import com.mysema.query.types.FactoryExpression;
import com.mysema.query.types.Operation;
import com.mysema.query.types.Path;
import com.mysema.query.types.PathType;
import com.mysema.query.types.SubQueryExpression;
import com.mysema.query.types.path.SimplePath;
/**
 * Class responsible for AST -> Linq expression serialization
 */
public class LinqSerializer extends SerializerBase<LinqSerializer>{

  public LinqSerializer(LinqQueryTemplates templates) {
    super(templates);
  }

  public String toLinq(LinqExpressionMixin<?> query) {
    handle(query.getExpression());
    return toString();
  }

  @Override
  public Void visit(Path< ? > path, Void context) {
    if (PathType.PROPERTY == path.getMetadata().getPathType()) {
      String propToUpper = StringUtils.capitalize(path.getMetadata().getName());
      SimplePath<?> newPath = new SimplePath<>(path.getType(), path.getMetadata().getParent(), propToUpper);
      return super.visit(newPath, context);
    }
    return super.visit(path, context);
  }

  @SuppressWarnings("unchecked")
  @Override
  public Void visit(FactoryExpression< ? > expr, Void context) {
    if (expr instanceof AnonymousExpression) {
      AnonymousExpression<?> anonymous = (AnonymousExpression<?>) expr;
      append("new {");
      for (int i = 0; i < anonymous.getParamsCount(); i++) {
        Operation< ? > operation = (Operation< ? >) anonymous.getArgs().get(i);
        Constant<String> propName = (Constant<String>) operation.getArg(0);
        Expression<?> selector = operation.getArg(1);

        append(propName.getConstant());
        append(" = ");
        handle(selector);
        if (i < anonymous.getParamsCount() - 1) {
          append(", ");
        }
      }

      append("}");
      return null;
    } else {
      return super.visit(expr, context);
    }
  }

  @Override
  public Void visit(SubQueryExpression< ? > expr, Void context) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void visitConstant(Object constant) {
    if (constant instanceof String) {
      append("\"").append(constant.toString()).append("\"");
    } else if (constant instanceof Number) {
      append(constant.toString());
    } else {
      super.visitConstant(constant);
    }
  }


}
