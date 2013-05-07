package raven.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Map;

import org.junit.Test;

import raven.client.json.JTokenType;
import raven.client.json.MapWithParentSnapshot;
import raven.client.json.RavenJToken;
import raven.client.json.RavenJValue;

public class MapWithParentSnapshotTest {
  @Test
  public void baseTest() {
    MapWithParentSnapshot map = new MapWithParentSnapshot();
    map.put("name", new RavenJValue("Marcin"));

    assertFalse(map.isEmpty());

    assertTrue("key name should be present in map", map.containsKey("name"));
    assertNotNull(map.get("name"));

    Map<String, RavenJToken> localChanges = map.getLocalChanges();
    assertEquals(1, localChanges.size());
    assertEquals(JTokenType.STRING, localChanges.get("name").getType());

    map.remove("name");
    assertTrue(map.isEmpty());

    localChanges = map.getLocalChanges();
    assertFalse(localChanges.isEmpty());
    assertNotNull("It should contain DELETE_MARKER", localChanges.get("name"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void canDuplicateKeys() {
    MapWithParentSnapshot map = new MapWithParentSnapshot();
    map.put("name", RavenJValue.parse("5"));
    map.put("name", RavenJValue.parse("5"));
  }

  @Test
  public void testClear() {
    MapWithParentSnapshot map = new MapWithParentSnapshot();
    map.put("name1", RavenJValue.parse("5"));
    map.put("name2", RavenJValue.parse("5"));
    map.clear();
    assertTrue(map.isEmpty());
  }

  @Test
  public void testWithParents() {
    MapWithParentSnapshot map = new MapWithParentSnapshot();
    map.put("name1", RavenJValue.parse("5"));
    map.ensureSnapshot("snap!");

    MapWithParentSnapshot snapshot = map.createSnapshot();

    assertFalse(snapshot.isEmpty());
    assertEquals(1, snapshot.size());
    assertTrue(snapshot.containsKey("name1"));
    assertNotNull(snapshot.get("name1"));

    snapshot.put("name2", RavenJValue.parse("7"));
    assertFalse(snapshot.isEmpty());
    assertEquals(2, snapshot.size());
    assertTrue(snapshot.containsKey("name1"));
    assertNotNull(snapshot.get("name1"));
    assertTrue(snapshot.containsKey("name2"));
    assertNotNull(snapshot.get("name2"));

    snapshot.remove("name1");
    assertFalse(snapshot.isEmpty());
    assertEquals(1, snapshot.size());
    assertTrue(snapshot.containsKey("name2"));
    assertNotNull(snapshot.get("name2"));

    snapshot.remove("name2");
    assertTrue(snapshot.isEmpty());
    assertEquals(0, snapshot.size());
    assertFalse(snapshot.containsKey("name1"));
    assertNull(snapshot.get("name1"));
    assertFalse(snapshot.containsKey("name2"));
    assertNull(snapshot.get("name2"));


  }
}
