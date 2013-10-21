package net.ravendb.abstractions.extensions;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import net.ravendb.abstractions.basic.SharpAwareJacksonAnnotationIntrospector;
import net.ravendb.abstractions.basic.SharpEnum;
import net.ravendb.abstractions.data.DocumentChangeTypes;
import net.ravendb.abstractions.data.EnumSet;
import net.ravendb.abstractions.data.Etag;
import net.ravendb.abstractions.data.FacetAggregation;
import net.ravendb.abstractions.data.FacetAggregationSet;
import net.ravendb.abstractions.data.IndexChangeTypes;
import net.ravendb.abstractions.data.ReplicationConflictTypes;
import net.ravendb.abstractions.data.ReplicationOperationTypes;
import net.ravendb.abstractions.indexing.SortOptions;
import net.ravendb.abstractions.json.linq.RavenJArray;
import net.ravendb.abstractions.json.linq.RavenJObject;
import net.ravendb.abstractions.json.linq.RavenJToken;
import net.ravendb.abstractions.json.linq.RavenJValue;
import net.ravendb.abstractions.util.NetDateFormat;
import net.ravendb.abstractions.util.ValueTypeUtils;
import net.ravendb.client.SearchOptions;
import net.ravendb.client.SearchOptionsSet;
import net.ravendb.client.document.FailoverBehavior;
import net.ravendb.client.document.FailoverBehaviorSet;

import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.JsonToken;
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


public class JsonExtensions {

  public static ObjectMapper createDefaultJsonSerializer() {
    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.setPropertyNamingStrategy(new DotNetNamingStrategy());
    objectMapper.disable(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES);
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

    module.addSerializer(FacetAggregationSet.class, new RavenEnumSetSerializer(FacetAggregation.class));
    module.addSerializer(FailoverBehaviorSet.class, new RavenEnumSetSerializer(FailoverBehavior.class));
    module.addSerializer(SearchOptionsSet.class, new RavenEnumSetSerializer(SearchOptions.class));

    module.addSerializer(RavenJObject.class, new RavenJTokenSerializer<>(RavenJObject.class));
    module.addSerializer(RavenJToken.class, new RavenJTokenSerializer<>(RavenJToken.class));
    module.addSerializer(RavenJArray.class, new RavenJTokenSerializer<>(RavenJArray.class));
    module.addSerializer(RavenJValue.class, new RavenJTokenSerializer<>(RavenJValue.class));

    module.addSerializer(SortOptions.class, new RavenEnumSerializer(SortOptions.class));
    module.addSerializer(DocumentChangeTypes.class, new RavenEnumSerializer(DocumentChangeTypes.class));
    module.addSerializer(FacetAggregation.class, new RavenEnumSerializer(FacetAggregation.class));
    module.addSerializer(IndexChangeTypes.class, new RavenEnumSerializer(IndexChangeTypes.class));
    module.addSerializer(ReplicationConflictTypes.class, new RavenEnumSerializer(ReplicationConflictTypes.class));
    module.addSerializer(ReplicationOperationTypes.class, new RavenEnumSerializer(ReplicationOperationTypes.class));
    module.addSerializer(SearchOptions.class, new RavenEnumSerializer(SearchOptions.class));
    module.addSerializer(FailoverBehavior.class, new RavenEnumSerializer(FailoverBehavior.class));

    module.addDeserializer(SortOptions.class, new RavenEnumDeserializer<>(SortOptions.class));
    module.addDeserializer(DocumentChangeTypes.class, new RavenEnumDeserializer<>(DocumentChangeTypes.class));
    module.addDeserializer(FacetAggregation.class, new RavenEnumDeserializer<>(FacetAggregation.class));
    module.addDeserializer(IndexChangeTypes.class, new RavenEnumDeserializer<>(IndexChangeTypes.class));
    module.addDeserializer(ReplicationConflictTypes.class, new RavenEnumDeserializer<>(ReplicationConflictTypes.class));
    module.addDeserializer(ReplicationOperationTypes.class,
      new RavenEnumDeserializer<>(ReplicationOperationTypes.class));
    module.addDeserializer(SearchOptions.class, new RavenEnumDeserializer<>(SearchOptions.class));
    module.addDeserializer(FailoverBehavior.class, new RavenEnumDeserializer<>(FailoverBehavior.class));

    return module;
  }

  public static class RavenEnumSerializer extends SerializerBase<Enum<?>> {

    @SuppressWarnings("unchecked")
    protected RavenEnumSerializer(Class<? extends Enum<?>> t) {
      super((Class<Enum<?>>) t);
    }

    @Override
    public void serialize(Enum<?> value, JsonGenerator jgen, SerializerProvider provider) throws IOException,
      JsonGenerationException {
      if (provider.isEnabled(Feature.WRITE_ENUMS_USING_INDEX)) {
        if (value == null) {
          jgen.writeNull();
          return;
        }
        try {
          Method method = value.getClass().getMethod("getValue");
          Integer intValue = (Integer) method.invoke(value, new Object[] {});
          jgen.writeNumber(intValue);
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
          throw new RuntimeException(e);
        }
      } else {
        jgen.writeString(SharpEnum.value(value));
      }
    }

  }

  @SuppressWarnings("rawtypes")
  public static class RavenEnumSetSerializer extends SerializerBase<EnumSet> {

    private Class<? extends Enum<?>> innerClass;

    public RavenEnumSetSerializer(Class<? extends Enum<?>> innerClass) {
      super(EnumSet.class);
      this.innerClass = innerClass;
    }

    private Map<Enum, Long> enumValues = new HashMap<>();
    private boolean cacheInitialized = false;

    private void initCache() {
      if (cacheInitialized) {
        return;
      }
      try {
        cacheInitialized = true;
        Method getValueMethod = innerClass.getMethod("getValue");

        for (Object o : innerClass.getEnumConstants()) {
          int enumValue = (int) getValueMethod.invoke(o);
          enumValues.put((Enum) o, Long.valueOf(enumValue));
        }
      } catch (NoSuchMethodException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
        throw new RuntimeException(e);
      }

    }

    @Override
    public void serialize(EnumSet value, JsonGenerator jgen, SerializerProvider provider) throws IOException,
      JsonGenerationException {
      if (provider.isEnabled(Feature.WRITE_ENUMS_USING_INDEX)) {
        jgen.writeNumber(value.getValue());
      } else {
        initCache();
        Object[] enumConstants = value.getInnerClass().getEnumConstants();
        Set<String> result = new LinkedHashSet<>();
        outer: for (Object item : enumConstants) {
          if (value.contains((Enum) item)) {
            // check if other value does not contain this one
            for (Object subItem : enumConstants) {
              long itemValue = enumValues.get(item);
              long subItemValue = enumValues.get(subItem);
              if (!subItem.equals(item) && value.contains((Enum) subItem)
                && ((itemValue | subItemValue) == subItemValue)) {
                continue outer;
              }
            }

            result.add(SharpEnum.value((Enum<?>) item));
          }
        }
        jgen.writeString(StringUtils.join(result, ", "));
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

    protected RavenJTokenDeserializer(Class<T> vc) {
      super(vc);
    }

    @Override
    public T deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException, JsonProcessingException {
      return (T) RavenJToken.load(jp);
    }
  }

  public static class RavenEnumDeserializer<T extends Enum<?>> extends StdDeserializer<T> {

    private Map<Long, T> cache = new HashMap<>();
    private Map<String, T> stringCache = new HashMap<>();

    public RavenEnumDeserializer(Class<T> vc) {
      super(vc);
    }

    @Override
    public T deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException, JsonProcessingException {
      try {
        initCache();

        if (jp.getCurrentToken() == JsonToken.VALUE_STRING) {
          String text = jp.getText();
          if (!stringCache.containsKey(text)) {
            throw new IOException("Unexpected text token: " + text);
          }
          return stringCache.get(text);
        } else {
          long longValue = jp.getValueAsInt();
          if (!cache.containsKey(longValue)) {
            throw new IOException("Unable to find matching enum value in cache for:" + longValue);
          }
          return cache.get(longValue);
        }
      } catch (Exception e) {
        throw new IOException(e);
      }
    }

    @SuppressWarnings("unchecked")
    private void initCache() throws NoSuchMethodException, SecurityException, IllegalAccessException,
      IllegalArgumentException, InvocationTargetException {
      if (!cache.isEmpty()) {
        return;
      }
      if (_valueClass.getEnumConstants().length == 0) {
        return;
      }

      Method getValueMethod = _valueClass.getMethod("getValue");

      for (Object o : _valueClass.getEnumConstants()) {
        int enumValue = (int) getValueMethod.invoke(o);
        cache.put(Long.valueOf(enumValue), (T) o);
        String enumStringValue = SharpEnum.value((Enum<?>) o);
        stringCache.put(enumStringValue, (T) o);
      }

    }

  }

  public static class EtagDeserializer extends FromStringDeserializer<Etag> {

    private EtagDeserializer(Class<?> vc) {
      super(vc);
    }

    @Override
    protected Etag _deserialize(String value, DeserializationContext ctxt) throws IOException, JsonProcessingException {
      return Etag.parse(value);
    }

  }

  public static class DotNetNamingStrategy extends PropertyNamingStrategy {

    @Override
    public String nameForField(MapperConfig<?> config, AnnotatedField field, String defaultName) {
      return StringUtils.capitalize(defaultName);
    }

    @Override
    public String nameForGetterMethod(MapperConfig<?> config, AnnotatedMethod method, String defaultName) {
      return StringUtils.capitalize(defaultName);
    }

    @Override
    public String nameForSetterMethod(MapperConfig<?> config, AnnotatedMethod method, String defaultName) {
      return StringUtils.capitalize(defaultName);
    }

    @Override
    public String nameForConstructorParameter(MapperConfig<?> config, AnnotatedParameter ctorParam, String defaultName) {
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
