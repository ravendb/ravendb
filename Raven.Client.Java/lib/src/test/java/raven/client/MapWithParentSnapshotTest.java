package raven.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.HashMap;
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

  @Test(expected = IllegalStateException.class)
  public void testCannotModifySnapshot() {
    MapWithParentSnapshot map = new MapWithParentSnapshot();
    map.ensureSnapshot();
    map.put("key", RavenJToken.fromObject("a"));
  }

  @Test
  public void testValues() {
    MapWithParentSnapshot map = new MapWithParentSnapshot();
    map.put("name1", RavenJValue.parse("5"));
    map.put("test", RavenJValue.parse("null"));
    map.put("@id", RavenJValue.fromObject("c"));
    map.remove("@id");
    map.ensureSnapshot("snap!");

    MapWithParentSnapshot snapshot = map.createSnapshot();
    snapshot.remove("name1");
    Collection<RavenJToken> collection = snapshot.values();
    assertEquals(1, collection.size());
    assertEquals(JTokenType.NULL, collection.iterator().next().getType());
  }

  @Test(expected =  IllegalStateException.class)
  public void testContainsValue() {
    MapWithParentSnapshot map = new MapWithParentSnapshot();
    map.containsValue("any");
  }

  @Test(expected =  IllegalStateException.class)
  public void testEntrySet() {
    MapWithParentSnapshot map = new MapWithParentSnapshot();
    map.entrySet();
  }

  @Test
  public void testPutAll() {
    MapWithParentSnapshot map = new MapWithParentSnapshot();
    Map<String, RavenJToken> toInsert = new HashMap<>();
    toInsert.put("#id", RavenJValue.parse("null"));
    map.putAll(toInsert);
    assertEquals(1, map.size());
  }

  @Test
  public void testEmptyKeyset() {
    MapWithParentSnapshot map = new MapWithParentSnapshot();
    assertTrue(map.keySet().isEmpty());
    map.ensureSnapshot();
    MapWithParentSnapshot snapshot = map.createSnapshot();
    assertTrue(snapshot.keySet().isEmpty());
  }

  @Test
  public void testWithParents() {
    MapWithParentSnapshot map = new MapWithParentSnapshot();
    map.put("name1", RavenJValue.parse("5"));
    map.put("@id", RavenJValue.fromObject("c"));
    map.remove("@id");
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

    assertFalse(map.containsKey("@id"));



  }
}
