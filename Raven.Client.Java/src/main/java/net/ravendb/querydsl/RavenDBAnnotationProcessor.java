package net.ravendb.querydsl;


import java.lang.annotation.Annotation;
import java.util.Collections;

import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;

import com.mysema.query.annotations.QueryEmbeddable;
import com.mysema.query.annotations.QueryEmbedded;
import com.mysema.query.annotations.QueryEntities;
import com.mysema.query.annotations.QueryEntity;
import com.mysema.query.annotations.QuerySupertype;
import com.mysema.query.annotations.QueryTransient;
import com.mysema.query.apt.Configuration;
import com.mysema.query.apt.QuerydslAnnotationProcessor;

@SupportedAnnotationTypes({"com.mysema.query.annotations.*"})
public class RavenDBAnnotationProcessor extends QuerydslAnnotationProcessor {

  @Override
  protected Configuration createConfiguration(RoundEnvironment roundEnv) {
    Class<? extends Annotation> entities = QueryEntities.class;
    Class<? extends Annotation> entity = QueryEntity.class;
    Class<? extends Annotation> superType = QuerySupertype.class;
    Class<? extends Annotation> embeddable = QueryEmbeddable.class;
    Class<? extends Annotation> embedded = QueryEmbedded.class;
    Class<? extends Annotation> skip = QueryTransient.class;

    RavenDefaultConfiguration defaultConfig = new RavenDefaultConfiguration(roundEnv, processingEnv.getOptions(),Collections.<String>emptySet(), entities,
        entity, superType, embeddable, embedded, skip);

    defaultConfig.addCustomType(String.class, RavenString.class);
    return defaultConfig;
  }

}
