package net.ravendb.abstractions.data;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.io.IOException;

import net.ravendb.abstractions.data.Etag;
import net.ravendb.abstractions.data.UuidType;
import net.ravendb.abstractions.extensions.JsonExtensions;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.junit.Test;


public class EtagTest {

  public static class EtagHolder {
    private Etag tag;

    public Etag getTag() {
      return tag;
    }

    public void setTag(Etag tag) {
      this.tag = tag;
    }


  }

  @Test
  public void testGenerateRandom() {
    Etag random = Etag.random();
    assertNotEquals(0L, random.getChanges());
  }

  @Test
  public void testParseEtagUsingDefaultFactor() throws JsonParseException, JsonMappingException, IOException {
    EtagHolder etagHolder = JsonExtensions.createDefaultJsonSerializer().readValue("{\"Tag\": \"00000001-0000-0100-0000-000000000002\"}", EtagHolder.class);
    assertEquals(Etag.parse("00000001-0000-0100-0000-000000000002"), etagHolder.tag);
  }


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
