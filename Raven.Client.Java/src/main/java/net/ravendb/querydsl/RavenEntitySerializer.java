package net.ravendb.querydsl;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import com.mysema.codegen.CodeWriter;
import com.mysema.codegen.model.ClassType;
import com.mysema.codegen.model.Type;
import com.mysema.query.codegen.EntitySerializer;
import com.mysema.query.codegen.EntityType;
import com.mysema.query.codegen.Property;
import com.mysema.query.codegen.SerializerConfig;
import com.mysema.query.codegen.TypeMappings;
import com.mysema.query.types.expr.SimpleExpression;
import com.mysema.query.types.path.ListPath;

public class RavenEntitySerializer extends EntitySerializer {

  public RavenEntitySerializer(TypeMappings mappings, Collection<String> keywords) {
    super(mappings, keywords);
  }

  @Override
  protected void outro(EntityType model, CodeWriter writer) throws IOException {
    writeCreateListMethod(model, writer);
    super.outro(model, writer);
  }

  private boolean hasListProperty(EntityType model) {
    for (Property property : model.getProperties()) {
      if (property.getType().getFullName().equals(List.class.getName())) {
        return true;
      }
    }

    return false;
  }

  private void writeCreateListMethod(EntityType model, CodeWriter writer) throws IOException {

    if (!hasListProperty(model)) {
      return;
    }
    writer.suppressWarnings("all");
    writer.append("    protected <A, E extends SimpleExpression<? super A>> RavenList<A, E> createList(String property, Class<? super A> type, Class<? super E> queryType, PathInits inits) {");
    writer.nl();
    writer.append("       return add(new RavenList<A, E>(type, (Class) queryType, forProperty(property), inits));");
    writer.nl();
    writer.append("    }");
    writer.nl();
  }



  @Override
  protected void introImports(CodeWriter writer, SerializerConfig config, EntityType model) throws IOException {
    super.introImports(writer, config, model);
    if (hasListProperty(model)) {
      writer.imports(SimpleExpression.class, RavenList.class);
    }
  }

  @Override
  protected void serialize(EntityType model, Property field, Type type, CodeWriter writer, String factoryMethod, String... args) throws IOException {
    if (type instanceof ClassType) {
      ClassType classType = (ClassType) type;
      if (classType.getJavaClass().equals(ListPath.class)) {
        type = new ClassType(type.getCategory(), RavenList.class, type.getParameters());
      }
    }
    super.serialize(model, field, type, writer, factoryMethod, args);
  }


}
