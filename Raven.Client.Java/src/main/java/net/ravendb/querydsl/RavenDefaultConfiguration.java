package net.ravendb.querydsl;

import java.lang.annotation.Annotation;
import java.util.Collection;
import java.util.Map;

import javax.annotation.processing.RoundEnvironment;

import com.mysema.query.apt.DefaultConfiguration;
import com.mysema.query.codegen.Serializer;

public class RavenDefaultConfiguration  extends DefaultConfiguration{

  private RavenEntitySerializer entitySerializer;

  public RavenDefaultConfiguration(RoundEnvironment roundEnv, Map<String, String> options, Collection<String> keywords, Class< ? extends Annotation> entitiesAnn,
      Class< ? extends Annotation> entityAnn, Class< ? extends Annotation> superTypeAnn, Class< ? extends Annotation> embeddableAnn, Class< ? extends Annotation> embeddedAnn,
      Class< ? extends Annotation> skipAnn) {
    super(roundEnv, options, keywords, entitiesAnn, entityAnn, superTypeAnn, embeddableAnn, embeddedAnn, skipAnn);
    entitySerializer = new RavenEntitySerializer(getTypeMappings(), getKeywords());
  }

  @Override
  public Serializer getEntitySerializer() {
    return entitySerializer;
  }

}
