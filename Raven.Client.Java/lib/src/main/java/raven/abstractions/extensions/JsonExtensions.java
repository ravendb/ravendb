package raven.abstractions.extensions;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.map.ObjectMapper;

public class JsonExtensions {
  private static ObjectMapper objectMapper;

  private static JsonFactory jsonFactory;

  private static void init() {
    synchronized (JsonExtensions.class) {
      if (objectMapper == null) {
        objectMapper = new ObjectMapper();
        jsonFactory = objectMapper.getJsonFactory();
      }
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
}
