package raven.imports.json;

import java.io.IOException;

import raven.abstractions.extensions.JsonExtensions;

//TODO: finish me
public class JsonConvert {
  public static String serializeObject(Object obj) {
    try {
      return JsonExtensions.getDefaultObjectMapper().writer().writeValueAsString(obj);
    } catch (IOException e) {
      throw new RuntimeException("Unable to serialize object.", e);
    }
  }
}
