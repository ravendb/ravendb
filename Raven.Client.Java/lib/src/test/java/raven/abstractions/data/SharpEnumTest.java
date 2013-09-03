package raven.abstractions.data;

import static org.junit.Assert.assertEquals;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;

import raven.abstractions.extensions.JsonExtensions;
import raven.abstractions.indexing.SortOptions;

public class SharpEnumTest {
  @Test
  public void testEnumReadWrite() throws Exception {
    ObjectMapper mapper = JsonExtensions.getDefaultObjectMapper();

    assertEquals("8", mapper.writeValueAsString(SortOptions.SHORT));
    assertEquals(SortOptions.SHORT, mapper.readValue("\"Short\"", SortOptions.class));
  }
}
