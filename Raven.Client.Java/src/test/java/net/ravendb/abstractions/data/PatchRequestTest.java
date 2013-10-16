package net.ravendb.abstractions.data;

import static org.junit.Assert.assertEquals;

import net.ravendb.abstractions.data.PatchCommandType;
import net.ravendb.abstractions.data.PatchRequest;
import net.ravendb.abstractions.json.linq.RavenJObject;
import net.ravendb.abstractions.json.linq.RavenJValue;

import org.junit.Test;


public class PatchRequestTest {
  @Test
  public void testConversion() {
    PatchRequest request = new PatchRequest();
    request.setName("requestName");
    request.setAllPositions(false);
    request.setPosition(10);
    request.setPrevVal(new RavenJValue("oldValue"));
    request.setType(PatchCommandType.COPY);
    request.setValue(new RavenJValue("newValue"));

    PatchRequest subRequest1 = new PatchRequest();
    subRequest1.setName("sub1");

    PatchRequest subRequest2 = new PatchRequest();
    subRequest2.setName("sub2");

    request.setNested(new PatchRequest[] { subRequest1, subRequest2 });

    RavenJObject json = request.toJson();

    PatchRequest coverted = PatchRequest.fromJson(json);
    assertEquals(2, coverted.getNested().length);
    assertEquals("requestName", coverted.getName());
  }
}
