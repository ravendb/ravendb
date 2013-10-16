package net.ravendb.client.linq;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import net.ravendb.abstractions.basic.Reference;
import net.ravendb.client.document.DocumentConvention;

import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.annotate.JsonProperty;


import com.mysema.query.types.Constant;
import com.mysema.query.types.Expression;
import com.mysema.query.types.Operation;
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

    if (expression instanceof Operation) {
      Operation<?> operation = (Operation< ? >) expression;
      if (operation.getOperator().equals(Ops.STRING_LENGTH) || operation.getOperator().equals(Ops.ARRAY_SIZE)) {
        Result result = getPath(operation.getArg(0));
        result.setPath(result.getPath() + ".Length");
        return result;
      } else if (operation.getOperator().equals(Ops.COL_SIZE)) {
        Result result = getPath(operation.getArg(0));
        result.setPath(result.getPath() + ".Count");
        return result;
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
        newResult.setPath(parent.getPath() + "." + getValueFromExpression(path, mapPath.getKeyType()));

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


        newResult.setMaybeProperty(fetchMaybeProperty(memberExpression));
        newResult.setPath(handlePropertyRenames(memberExpression, newResult.getPath()));
        return newResult;
      }
    }
    throw new IllegalArgumentException("Don't know how to translate: " + expression);
  }

  private Field fetchMaybeProperty(Path<?> memberExpression) {
    try {
      Path<?> parent = memberExpression.getMetadata().getParent();
      if (parent == null) {
        return null;
      }
      String fieldName = memberExpression.getMetadata().getName();
      Field declaredField = null;
      Class<?> type = parent.getType();
      while (true) {
        declaredField = type.getDeclaredField(fieldName);
        if (declaredField != null) {
          return declaredField;
        }
        type = type.getSuperclass();
        if (Object.class.equals(type)) {
          return null;
        }
      }
    } catch (NoSuchFieldException e) {
      throw new RuntimeException(e);
    }
  }

  private String extractPath(Path<?> expression) {
    List<String> items = new ArrayList<>();

    while (expression != null) {
      if (expression.getMetadata().getPathType() == PathType.DELEGATE) {
        expression = expression.getMetadata().getParent();
        continue;
      }
      items.add(0, StringUtils.capitalize(expression.getMetadata().getName()));
      expression = expression.getMetadata().getParent();
    }
    return StringUtils.join(items, ".");
  }

  public static String handlePropertyRenames(Path<?> member, String name) {
    JsonProperty jsonProperty = member.getAnnotatedElement().getAnnotation(JsonProperty.class);
    if (jsonProperty != null) {
      String propertyName = jsonProperty.value();
      if (StringUtils.isNotEmpty(propertyName)) {
        return name.substring(0, name.length() - member.getMetadata().getName().length()) + propertyName;
      }
    }
    return name;
  }

  private static Expression<?> simplifyExpression(Expression<?> expression) {
    // do nothing
    return expression;
  }

  public Object getValueFromExpression(Expression<?> expression, Class< ? > type) {
    if (expression == null) {
      return new IllegalArgumentException("Value is missing");
    }
    // get object
    Reference<Object> valueRef= new Reference<>();
    if (getValueFromExpressionWithoutConversion(expression, valueRef)) {
      if (valueRef.value == null) {
        return null;
      }
      if (valueRef.value instanceof Enum) {
        if (!conventions.isSaveEnumsAsIntegers()) {
          return ((Enum<?>)valueRef.value).name();
        }
        return ((Enum<?>)valueRef.value).ordinal();
      }

      return valueRef.value;
    }
    throw new IllegalStateException("Can't extract value from expression of type:" + expression);
  }

  public Path<?> getMemberExpression(Expression<?> expression) {
    if (expression instanceof Path) {
      return (Path< ? >) expression;
    }
    throw new IllegalStateException("Could not understand how to translate '" + expression + "' to a RavenDB query.\n" +
      "Are you trying to do computation during the query?\n" +
      "RavenDB doesn't allow computation during the query, computation is only allowed during index. Consider moving the operation to an index."
      );
  }

  public static boolean getValueFromExpressionWithoutConversion(Expression<?> expression, Reference<Object> valueRef) {
    if (expression instanceof Constant) {
      valueRef.value = ((Constant<?>) expression).getConstant();
      return true;
    }
    return false;
  }

  private void assertNoComputation(Path<?> expression) {
    //empty
  }

}
