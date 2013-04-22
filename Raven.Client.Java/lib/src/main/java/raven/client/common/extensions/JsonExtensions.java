package raven.client.common.extensions;

import org.codehaus.jackson.map.ObjectMapper;

public class JsonExtensions {
  private static ObjectMapper objectMapper;

  public static ObjectMapper getDefaultObjectMapper() {
    if (objectMapper == null) {
      synchronized (JsonExtensions.class) {
       if (objectMapper == null) {
         objectMapper = new ObjectMapper();
       }
      }
    }
    return objectMapper;
  }
}
