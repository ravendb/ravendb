package raven.client.linq;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import raven.abstractions.basic.Reference;
import raven.client.document.DocumentConvention;

import com.mysema.query.types.Constant;
import com.mysema.query.types.Expression;
import com.mysema.query.types.Ops;
import com.mysema.query.types.Path;
import com.mysema.query.types.PathType;
import com.mysema.query.types.expr.NumberOperation;
import com.mysema.query.types.path.MapPath;

public class LinqPathProvider {
  public static class Result {
    private Class<?> memberType;
    private String path;
    private boolean isNestedPath;
    private Field maybeProperty;

    public Field getMaybeProperty() {
      return maybeProperty;
    }
    public Class< ? > getMemberType() {
      return memberType;
    }
    public String getPath() {
      return path;
    }
    public boolean isNestedPath() {
      return isNestedPath;
    }
    public void setMaybeProperty(Field maybeProperty) {
      this.maybeProperty = maybeProperty;
    }
    public void setMemberType(Class< ? > memberType) {
      this.memberType = memberType;
    }
    public void setNestedPath(boolean isNestedPath) {
      this.isNestedPath = isNestedPath;
    }
    public void setPath(String path) {
      this.path = path;
    }

  }

  private final DocumentConvention conventions;

  public LinqPathProvider(DocumentConvention documentConvention) {
    this.conventions = documentConvention;
  }

  public Result getPath(Expression< ? > expression) {
    expression = simplifyExpression(expression);

    if (expression instanceof NumberOperation<?>) {
      Result customMethodResult = conventions.translateCustomQueryExpression(this, expression);
      if (customMethodResult != null) {
        return customMethodResult;
      }

      NumberOperation<?> numberOperation = (NumberOperation<?>) expression;
      if (numberOperation.getOperator().equals(Ops.MAP_SIZE) || numberOperation.getOperator().equals(Ops.ARRAY_SIZE)) {
        if (numberOperation.getArgs().size() != 1) {
          throw new IllegalArgumentException("Invalid computation: " + numberOperation
              + ". You cannot use computation (only simple member expression are allowed) in RavenDB queries.");
        }
        Result target = getPath(numberOperation.getArg(0));

        Result newResult = new Result();
        newResult.setMemberType(numberOperation.getType());
        newResult.setNestedPath(false);
        newResult.setPath(target.getPath() + ".Count\\(\\)");
        return newResult;
      }

    }

    if (expression instanceof Path) {
      Path<?> path = (Path<?>) expression;

      PathType pathType = path.getMetadata().getPathType();
      if (PathType.MAPVALUE_CONSTANT.equals(pathType)) {
        MapPath< ? , ?, ?> mapPath = (MapPath< ? , ? , ? >) path.getMetadata().getParent();
        Result parent = getPath(mapPath);
        Result newResult = new Result();
        newResult.setMemberType(expression.getType());
        newResult.setNestedPath(false);
        newResult.setPath(parent.getPath() + "." + getValueFromExpression(path.getMetadata().getElement(), mapPath.getKeyType()));

        return newResult;
      }
      if (PathType.PROPERTY.equals(pathType) || PathType.VARIABLE.equals(pathType)) {


        Path< ? > memberExpression = getMemberExpression(expression);
        Result customMemberResult = conventions.translateCustomQueryExpression(this, memberExpression);
        if (customMemberResult != null) {
          return customMemberResult;
        }

        assertNoComputation(memberExpression);

        Result newResult = new Result();

        newResult.setPath(extractPath(memberExpression));
        newResult.setNestedPath(memberExpression.getMetadata().getParent().getMetadata().getPathType().equals(PathType.PROPERTY));
        newResult.setMemberType(memberExpression.getType());
        //TODO:newResult.setMaybeProperty(memberExpression.get) do we need it?

        newResult.setPath(handlePropertyRenames(memberExpression, newResult.getPath()));
        return newResult;
      }

    }
    throw new IllegalArgumentException("Don't know how to translate: " + expression);
  }

  private String extractPath(Path<?> expression) {
    List<String> items = new ArrayList<>();

    while (expression != null) {
      items.add(0, StringUtils.capitalize(expression.getMetadata().getName()));
      expression = expression.getMetadata().getParent();
    }
    return StringUtils.join(items, ".");
  }

  public static String handlePropertyRenames(Path<?> member, String name) {
    // TODO Auto-generated method stub
    return name;
  }

  private static Expression<?> simplifyExpression(Expression<?> expression) {
    // do nothing
    return expression;
  }

  public Object getValueFromExpression(Object object, Class< ? > type) {
    if (object == null) {
      return new IllegalArgumentException("Value is missing");
    }
    // TODO Auto-generated method stub
    if (object instanceof Constant<?>) {
      return ((Constant<?>)object).getConstant();
    }

    return object;
  }

  public Path<?> getMemberExpression(Expression<?> expression) {

    //TODO: finish me
    return (Path< ? >) expression;
  }

  public static boolean getValueFromExpressionWithoutConversion(Expression<?> expression, Reference<Object> valueRef) {
    // TODO Auto-generated method stub
    return false;
  }

  private static Object getNewExpressionValue(Expression<?> expression) {
    // TODO Auto-generated method stub
    return null;
  }

  private static Object getMemberValue(Path<?> memberExpression) {
    // TODO Auto-generated method stub
    return null;
  }

  private void assertNoComputation(Path<?> expression) {

  }

}
