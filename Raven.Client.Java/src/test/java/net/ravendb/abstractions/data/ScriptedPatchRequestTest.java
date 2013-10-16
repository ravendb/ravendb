package net.ravendb.abstractions.data;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import net.ravendb.abstractions.data.ScriptedPatchRequest;
import net.ravendb.abstractions.extensions.JsonExtensions;
import net.ravendb.abstractions.json.linq.RavenJObject;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.junit.Test;


public class ScriptedPatchRequestTest {

  @Test
  public void testScriptedPatchRequest() throws JsonGenerationException, JsonMappingException, IOException {
    Map<String, Object> values = new HashMap<>();
    values.put("key1", 10);
    values.put("key2", "string");

    ScriptedPatchRequest request= new ScriptedPatchRequest();
    request.setScript("javascript");
    request.setValues(values);

    String valueAsString = JsonExtensions.createDefaultJsonSerializer().writeValueAsString(request);

    ScriptedPatchRequest parsedRequest = ScriptedPatchRequest.fromJson(RavenJObject.parse(valueAsString));
    assertEquals("javascript", parsedRequest.getScript());
    assertEquals(2, parsedRequest.getValues().size());


  }
}
