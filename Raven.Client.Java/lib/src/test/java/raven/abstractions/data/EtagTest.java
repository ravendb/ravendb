package raven.abstractions.data;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class EtagTest {

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
