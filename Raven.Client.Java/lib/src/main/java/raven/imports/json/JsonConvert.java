package raven.imports.json;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import raven.abstractions.extensions.JsonExtensions;
import raven.abstractions.json.linq.RavenJArray;
import raven.abstractions.json.linq.RavenJObject;
import raven.abstractions.json.linq.RavenJToken;

public class JsonConvert {
  public static String serializeObject(Object obj) {
    try {
      return JsonExtensions.createDefaultJsonSerializer().writer().writeValueAsString(obj);
    } catch (IOException e) {
      throw new RuntimeException("Unable to serialize object.", e);
    }
  }

  /**
   * This method gets RavenJArray, extracts propertyName from each object and maps to targetClass
   * @param array
   * @param targetClass
   * @param nestedPath
   * @return
   * @throws IOException
   * @throws JsonMappingException
   * @throws JsonParseException
   */
  public static <T> Collection<T> deserializeObject(RavenJArray array, Class<T> targetClass, String propertyName) throws JsonParseException, JsonMappingException, IOException {
    List<T> result = new ArrayList<>();

    ObjectMapper objectMapper = JsonExtensions.createDefaultJsonSerializer();

    for (RavenJToken token: array) {
      RavenJObject object = (RavenJObject) token;
      RavenJToken ravenJToken = object.get(propertyName);
      result.add(objectMapper.readValue(ravenJToken.toString(), targetClass));
    }
    return result;
  }



}
