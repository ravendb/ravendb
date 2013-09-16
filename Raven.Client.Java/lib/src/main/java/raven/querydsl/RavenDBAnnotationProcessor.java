package raven.querydsl;

import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;

import com.mysema.query.apt.Configuration;
import com.mysema.query.apt.DefaultConfiguration;
import com.mysema.query.apt.QuerydslAnnotationProcessor;

@SupportedAnnotationTypes({"com.mysema.query.annotations.*"})
public class RavenDBAnnotationProcessor extends QuerydslAnnotationProcessor {

  @Override
  protected Configuration createConfiguration(RoundEnvironment roundEnv) {
    DefaultConfiguration defaultConfig = (DefaultConfiguration) super.createConfiguration(roundEnv);
    defaultConfig.addCustomType(String.class, RavenString.class);
    return defaultConfig;
  }

}
