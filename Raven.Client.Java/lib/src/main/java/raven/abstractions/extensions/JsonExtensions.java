package raven.abstractions.extensions;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.Version;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.DeserializationContext;
import org.codehaus.jackson.map.MapperConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.PropertyNamingStrategy;
import org.codehaus.jackson.map.deser.std.FromStringDeserializer;
import org.codehaus.jackson.map.deser.std.StdDeserializer;
import org.codehaus.jackson.map.introspect.AnnotatedField;
import org.codehaus.jackson.map.introspect.AnnotatedMethod;
import org.codehaus.jackson.map.introspect.AnnotatedParameter;
import org.codehaus.jackson.map.module.SimpleModule;

import raven.abstractions.basic.SharpAwareJacksonAnnotationIntrospector;
import raven.abstractions.data.Etag;
import raven.abstractions.json.linq.RavenJArray;
import raven.abstractions.json.linq.RavenJObject;
import raven.abstractions.json.linq.RavenJToken;
import raven.abstractions.json.linq.RavenJValue;
import raven.abstractions.util.ValueTypeUtils;

public class JsonExtensions {
  private static ObjectMapper objectMapper;

  private static JsonFactory jsonFactory;

  private static void init() {
    synchronized (JsonExtensions.class) {
      if (objectMapper == null) {
        objectMapper = new ObjectMapper();
        objectMapper.setPropertyNamingStrategy(new DotNetNamingStrategy());
        objectMapper.disable(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES);
        objectMapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
        jsonFactory = objectMapper.getJsonFactory();

        objectMapper.registerModule(createCustomSerializeModule());
        objectMapper.setAnnotationIntrospector(new SharpAwareJacksonAnnotationIntrospector());
      }
    }
  }

  private static SimpleModule createCustomSerializeModule() {
    SimpleModule module = new SimpleModule("customSerializers", new Version(1, 0, 0, null));
    module.addDeserializer(Etag.class, new EtagDeserializer(Etag.class));
    module.addDeserializer(RavenJObject.class, new RavenJTokenDeserializer<RavenJObject>(RavenJObject.class));
    module.addDeserializer(RavenJToken.class, new RavenJTokenDeserializer<RavenJToken>(RavenJToken.class));
    module.addDeserializer(RavenJArray.class, new RavenJTokenDeserializer<RavenJArray>(RavenJArray.class));
    module.addDeserializer(RavenJValue.class, new RavenJTokenDeserializer<RavenJValue>(RavenJValue.class));
    return module;
  }

  public static class RavenJTokenDeserializer<T extends RavenJToken> extends StdDeserializer<T> {

    protected RavenJTokenDeserializer(Class< T > vc) {
      super(vc);
    }

    @SuppressWarnings("unchecked")
    @Override
    public T deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException, JsonProcessingException {
      return (T) RavenJToken.load(jp);
    }

  }


  public static class EtagDeserializer extends FromStringDeserializer<Etag> {

    private EtagDeserializer(Class< ? > vc) {
      super(vc);
    }

    @Override
    protected Etag _deserialize(String value, DeserializationContext ctxt) throws IOException, JsonProcessingException {
      return Etag.parse(value);
    }

  }

  public static ObjectMapper getDefaultObjectMapper() {
    if (objectMapper == null) {
      init();
    }
    return objectMapper;
  }

  public static JsonFactory getDefaultJsonFactory() {
    if (objectMapper == null) {
      init();
    }
    return jsonFactory;
  }

  public static class DotNetNamingStrategy extends PropertyNamingStrategy {

    @Override
    public String nameForField(MapperConfig< ? > config, AnnotatedField field, String defaultName) {
      return StringUtils.capitalize(defaultName);
    }

    @Override
    public String nameForGetterMethod(MapperConfig< ? > config, AnnotatedMethod method, String defaultName) {
      if (method.getAnnotated().getReturnType() == Boolean.TYPE) {
        defaultName = "is" + StringUtils.capitalize(defaultName);
      }
      return StringUtils.capitalize(defaultName);
    }

    @Override
    public String nameForSetterMethod(MapperConfig< ? > config, AnnotatedMethod method, String defaultName) {
      if (method.getParameterCount() == 1 && method.getParameterClass(0).equals(Boolean.TYPE)) {
        defaultName = "is" + StringUtils.capitalize(defaultName);
      }
      return StringUtils.capitalize(defaultName);
    }

    @Override
    public String nameForConstructorParameter(MapperConfig< ? > config, AnnotatedParameter ctorParam, String defaultName) {
      return StringUtils.capitalize(defaultName);
    }


  }


  public static RavenJObject toJObject(Object result) {
    if (result instanceof String || ValueTypeUtils.isValueType(result.getClass())) {
      RavenJObject jObject = new RavenJObject();
      jObject.add("Value", new RavenJValue(result));
      return jObject;
    }
    return RavenJObject.fromObject(result);
  }

}
