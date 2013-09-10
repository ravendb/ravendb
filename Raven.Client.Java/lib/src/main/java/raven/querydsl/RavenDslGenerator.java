package raven.querydsl;

import java.lang.annotation.Annotation;
import java.util.Arrays;

import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;

import com.mysema.query.annotations.QueryEmbeddable;
import com.mysema.query.annotations.QueryEmbedded;
import com.mysema.query.annotations.QueryEntities;
import com.mysema.query.annotations.QueryEntity;
import com.mysema.query.annotations.QuerySupertype;
import com.mysema.query.annotations.QueryTransient;
import com.mysema.query.apt.Configuration;
import com.mysema.query.apt.DefaultConfiguration;


@SupportedAnnotationTypes({"com.mysema.query.annotations.*"})
public class RavenDslGenerator extends com.mysema.query.apt.QuerydslAnnotationProcessor {

  public final static String[] KEYWORDS = new String[] { "ANY" };

  @Override
  protected Configuration createConfiguration(RoundEnvironment roundEnv) {
      Class<? extends Annotation> entities = QueryEntities.class;
      Class<? extends Annotation> entity = QueryEntity.class;
      Class<? extends Annotation> superType = QuerySupertype.class;
      Class<? extends Annotation> embeddable = QueryEmbeddable.class;
      Class<? extends Annotation> embedded = QueryEmbedded.class;
      Class<? extends Annotation> skip = QueryTransient.class;

      return new DefaultConfiguration(
              roundEnv, processingEnv.getOptions(), Arrays.asList(KEYWORDS), entities,
              entity, superType, embeddable, embedded, skip);
  }
}
