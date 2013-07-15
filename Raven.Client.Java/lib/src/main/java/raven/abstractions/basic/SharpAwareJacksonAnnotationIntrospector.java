package raven.abstractions.basic;

import org.codehaus.jackson.map.introspect.JacksonAnnotationIntrospector;

/**
 * Performs custom enum serialization for enums annotated with {@link UseSharpEnum}
 *
 */
public class SharpAwareJacksonAnnotationIntrospector extends JacksonAnnotationIntrospector {

  @Override
  public String findEnumValue(Enum< ? > value) {
    if (value.getClass().getAnnotation(UseSharpEnum.class) != null) {
      return SharpEnum.value(value);
    }
    return super.findEnumValue(value);
  }

}
