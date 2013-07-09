package raven.abstractions.data;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.junit.Test;

import raven.abstractions.extensions.JsonExtensions;

public class EtagTest {

  private static class EtagHolder {
    private Etag tag;

    public Etag getTag() {
      return tag;
    }

    public void setTag(Etag tag) {
      this.tag = tag;
    }

  }

  @Test
  public void testParseEtagUsingDefaultFactor() throws JsonParseException, JsonMappingException, IOException {
    EtagHolder etagHolder = JsonExtensions.getDefaultObjectMapper().readValue("{\"Tag\": \"00000001-0000-0100-0000-000000000002\"}", EtagHolder.class);
    assertEquals(Etag.parse("00000001-0000-0100-0000-000000000002"), etagHolder.tag);
  }


  //TODO: finish tests
  @Test
  public void testEmpty() {
    assertEquals("00000000-0000-0000-0000-000000000000", Etag.empty().toString());
  }

  @Test
  public void testToString() {
    assertEquals("01000000-0000-0005-0000-000000000009", new Etag(UuidType.DOCUMENTS, 5 , 9).toString());
  }

  @Test
  public void testTryParse() {
    Etag etag = Etag.parse("00000000-0000-0005-0000-000000000009");
    assertEquals(5L, etag.getRestarts());
    assertEquals(9L, etag.getChanges());
  }
}
