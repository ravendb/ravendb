package raven.abstractions.data;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Date;

import org.junit.Test;

import raven.abstractions.json.linq.RavenJObject;
import raven.abstractions.json.linq.RavenJValue;

public class JsonDocumentTest {
  @Test
  public void testToJson() {
    RavenJObject doc = new RavenJObject();
    doc.add("Name", new RavenJValue("John"));
    doc.add("LastName", new RavenJValue("Smith"));

    RavenJObject meta = new RavenJObject();
    meta.add("Content-Type", new RavenJValue("application/json"));

    Date now = new Date();

    JsonDocument document = new JsonDocument(doc, meta, "persons/1", true, Etag.empty(), now);

    RavenJObject ravenJObject = document.toJson();

    assertNotNull(ravenJObject.get("@metadata"));
    assertEquals("John", ravenJObject.value(String.class, "Name"));
    assertEquals(5, ravenJObject.value(RavenJObject.class, "@metadata").getCount());

  }
}
