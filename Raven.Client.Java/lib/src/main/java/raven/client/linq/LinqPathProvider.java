package raven.client.linq;

import java.beans.PropertyDescriptor;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.mysema.query.types.Expression;
import com.mysema.query.types.Path;

import raven.client.document.DocumentConvention;

public class LinqPathProvider {
  public static class Result {
    private Class<?> memberType;
    private String path;
    private boolean isNestedPath;
    private PropertyDescriptor maybeProperty;
    public PropertyDescriptor getMaybeProperty() {
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
    public void setMaybeProperty(PropertyDescriptor maybeProperty) {
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
    //TODO: implement me
    if (expression instanceof Path) {
        Path<?> path = (Path<?>) expression;
        List<String> tokens = new ArrayList<>();
        while (path != null) {
          String name = path.getMetadata().getName();
          path = path.getMetadata().getParent();
          if (path != null) {
            name = StringUtils.capitalize(name);
          }
          tokens.add(0, name);
        }
        Result result = new Result();
        result.setPath(StringUtils.join(tokens, "."));
        return result;
    } else {
      throw new RuntimeException("Expected Path expression. Got" + expression.getClass());
    }
  }

}
