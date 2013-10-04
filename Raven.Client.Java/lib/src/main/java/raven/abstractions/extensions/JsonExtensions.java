package raven.abstractions.extensions;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.EnumSet;
import java.util.Iterator;

import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.Version;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.DeserializationContext;
import org.codehaus.jackson.map.MapperConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.PropertyNamingStrategy;
import org.codehaus.jackson.map.SerializationConfig;
import org.codehaus.jackson.map.SerializationConfig.Feature;
import org.codehaus.jackson.map.SerializerProvider;
import org.codehaus.jackson.map.deser.std.FromStringDeserializer;
import org.codehaus.jackson.map.deser.std.StdDeserializer;
import org.codehaus.jackson.map.introspect.AnnotatedField;
import org.codehaus.jackson.map.introspect.AnnotatedMethod;
import org.codehaus.jackson.map.introspect.AnnotatedParameter;
import org.codehaus.jackson.map.module.SimpleModule;
import org.codehaus.jackson.map.ser.std.SerializerBase;
import org.codehaus.jackson.type.JavaType;

import raven.abstractions.basic.SerializeUsingValue;
import raven.abstractions.basic.SharpAwareJacksonAnnotationIntrospector;
import raven.abstractions.data.Etag;
import raven.abstractions.indexing.SortOptions;
import raven.abstractions.json.linq.RavenJArray;
import raven.abstractions.json.linq.RavenJObject;
import raven.abstractions.json.linq.RavenJToken;
import raven.abstractions.json.linq.RavenJValue;
import raven.abstractions.util.NetDateFormat;
import raven.abstractions.util.ValueTypeUtils;

public class JsonExtensions {

  public static ObjectMapper createDefaultJsonSerializer() {
    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.setPropertyNamingStrategy(new DotNetNamingStrategy());
    objectMapper.disable(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES);
    objectMapper.enable(Feature.WRITE_ENUMS_USING_INDEX);
    objectMapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
    objectMapper.disable(SerializationConfig.Feature.FAIL_ON_EMPTY_BEANS);
    objectMapper.configure(Feature.WRITE_DATES_AS_TIMESTAMPS, false);
    objectMapper.setSerializationConfig(objectMapper.getSerializationConfig().withDateFormat(new NetDateFormat()));
    objectMapper.setDeserializationConfig(objectMapper.getDeserializationConfig().withDateFormat(new NetDateFormat()));

    objectMapper.registerModule(createCustomSerializeModule());
    objectMapper.setAnnotationIntrospector(new SharpAwareJacksonAnnotationIntrospector());
    return objectMapper;
  }

  private static SimpleModule createCustomSerializeModule() {
    SimpleModule module = new SimpleModule("customSerializers", new Version(1, 0, 0, null));
    module.addDeserializer(Etag.class, new EtagDeserializer(Etag.class));
    module.addDeserializer(RavenJObject.class, new RavenJTokenDeserializer<>(RavenJObject.class));
    module.addDeserializer(RavenJToken.class, new RavenJTokenDeserializer<>(RavenJToken.class));
    module.addDeserializer(RavenJArray.class, new RavenJTokenDeserializer<>(RavenJArray.class));
    module.addDeserializer(RavenJValue.class, new RavenJTokenDeserializer<>(RavenJValue.class));
    module.addSerializer(EnumSet.class, new RavenEnumSetSerializer());
    module.addSerializer(RavenJObject.class, new RavenJTokenSerializer<>(RavenJObject.class));
    module.addSerializer(RavenJToken.class, new RavenJTokenSerializer<>(RavenJToken.class));
    module.addSerializer(RavenJArray.class, new RavenJTokenSerializer<>(RavenJArray.class));
    module.addSerializer(RavenJValue.class, new RavenJTokenSerializer<>(RavenJValue.class));
    //TODO: add deserializer for enumset and enum!
    module.addSerializer(SortOptions.class, new RavenEnumSerializer(SortOptions.class));
    return module;
  }

  public static class RavenEnumSerializer extends SerializerBase<Enum<?>> {

    @SuppressWarnings("unchecked")
    protected RavenEnumSerializer(Class<? extends Enum<?>> t) {
      super((Class<Enum< ? >>) t);
    }

    @Override
    public void serialize(Enum< ? > value, JsonGenerator jgen, SerializerProvider provider) throws IOException, JsonGenerationException {
      if (value == null) {
        jgen.writeNull();
        return;
      }
      try {
        Method method = value.getClass().getMethod("getValue");
        Integer intValue = (Integer) method.invoke(value, new Object[] { } );
        jgen.writeNumber(intValue);
      } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    }

  }

  @SuppressWarnings("rawtypes")
  public static class RavenEnumSetSerializer extends SerializerBase<EnumSet> {

    protected RavenEnumSetSerializer() {
      super(EnumSet.class);
    }

    @Override
    public void serialize(EnumSet value, JsonGenerator jgen, SerializerProvider provider) throws IOException, JsonGenerationException {
      try {
        if (value.isEmpty()) {
          jgen.writeNumber(0);
        } else {
          Enum firstEnum= (Enum) value.iterator().next();
          SerializeUsingValue serializeAsFlags = firstEnum.getClass().getAnnotation(SerializeUsingValue.class);
          if (serializeAsFlags != null) {
            Method method = firstEnum.getClass().getMethod("getValue");
            int result = 0;
            Iterator<Enum> iterator = value.iterator();
            while (iterator.hasNext()) {
              Object next = iterator.next();
              result |= (int)method.invoke(next);
            }
            jgen.writeNumber(result);
          } else {
            throw new IllegalStateException("not implemented yet");//TODO
          }
        }
      } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
        throw new RuntimeException(e);
      }

    }
  }

  public static class RavenJTokenSerializer<T extends RavenJToken> extends SerializerBase<T> {

    public RavenJTokenSerializer(Class<?> t, boolean dummy) {
      super(t, dummy);
    }

    public RavenJTokenSerializer(Class<T> t) {
      super(t);
    }

    public RavenJTokenSerializer(JavaType type) {
      super(type);
    }

    @Override
    public void serialize(T value, JsonGenerator jgen, SerializerProvider provider) throws IOException,
      JsonGenerationException {
      jgen.writeString(value.toString());
    }

  }

  public static class RavenJTokenDeserializer<T extends RavenJToken> extends StdDeserializer<T> {

    protected RavenJTokenDeserializer(Class< T > vc) {
      super(vc);
    }

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

  public static class DotNetNamingStrategy extends PropertyNamingStrategy {

    @Override
    public String nameForField(MapperConfig< ? > config, AnnotatedField field, String defaultName) {
      return StringUtils.capitalize(defaultName);
    }

    @Override
    public String nameForGetterMethod(MapperConfig< ? > config, AnnotatedMethod method, String defaultName) {
      return StringUtils.capitalize(defaultName);
    }

    @Override
    public String nameForSetterMethod(MapperConfig< ? > config, AnnotatedMethod method, String defaultName) {
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
