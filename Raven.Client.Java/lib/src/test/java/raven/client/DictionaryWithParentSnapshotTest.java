package raven.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.junit.Test;

import raven.abstractions.json.linq.DictionaryWithParentSnapshot;
import raven.abstractions.json.linq.JTokenType;
import raven.abstractions.json.linq.RavenJToken;
import raven.abstractions.json.linq.RavenJValue;

public class DictionaryWithParentSnapshotTest {
  @Test
  public void baseTest() {
    DictionaryWithParentSnapshot map = new DictionaryWithParentSnapshot();
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
    DictionaryWithParentSnapshot map = new DictionaryWithParentSnapshot();
    map.put("name", RavenJValue.parse("5"));
    map.put("name", RavenJValue.parse("5"));
  }

  @Test
  public void testClear() {
    DictionaryWithParentSnapshot map = new DictionaryWithParentSnapshot();
    map.put("name1", RavenJValue.parse("5"));
    map.put("name2", RavenJValue.parse("5"));
    map.clear();
    assertTrue(map.isEmpty());
  }

  @Test(expected = IllegalStateException.class)
  public void testCannotModifySnapshot() {
    DictionaryWithParentSnapshot map = new DictionaryWithParentSnapshot();
    map.ensureSnapshot();
    map.put("key", RavenJToken.fromObject("a"));
  }

  @Test
  public void testValues() {
    DictionaryWithParentSnapshot map = new DictionaryWithParentSnapshot();
    map.put("name1", RavenJValue.parse("5"));
    map.put("test", RavenJValue.parse("null"));
    map.put("@id", RavenJValue.fromObject("c"));
    map.remove("@id");
    map.ensureSnapshot("snap!");

    DictionaryWithParentSnapshot snapshot = map.createSnapshot();
    snapshot.remove("name1");
    Collection<RavenJToken> collection = snapshot.values();
    assertEquals(1, collection.size());
    assertEquals(JTokenType.NULL, collection.iterator().next().getType());
  }

  @Test(expected =  IllegalStateException.class)
  public void testContainsValue() {
    DictionaryWithParentSnapshot map = new DictionaryWithParentSnapshot();
    map.containsValue("any");
  }

  @Test(expected =  IllegalStateException.class)
  public void testEntrySet() {
    DictionaryWithParentSnapshot map = new DictionaryWithParentSnapshot();
    map.entrySet();
  }

  @Test
  public void testPutAll() {
    DictionaryWithParentSnapshot map = new DictionaryWithParentSnapshot();
    Map<String, RavenJToken> toInsert = new HashMap<>();
    toInsert.put("#id", RavenJValue.parse("null"));
    map.putAll(toInsert);
    assertEquals(1, map.size());
  }

  @Test
  public void testEmptyKeyset() {
    DictionaryWithParentSnapshot map = new DictionaryWithParentSnapshot();
    assertTrue(map.keySet().isEmpty());
    map.ensureSnapshot();
    DictionaryWithParentSnapshot snapshot = map.createSnapshot();
    assertTrue(snapshot.keySet().isEmpty());
  }

  @Test
  public void testWithParents() {
    DictionaryWithParentSnapshot map = new DictionaryWithParentSnapshot();
    map.put("name1", RavenJValue.parse("5"));
    map.put("@id", RavenJValue.fromObject("c"));
    map.remove("@id");
    map.ensureSnapshot("snap!");

    DictionaryWithParentSnapshot snapshot = map.createSnapshot();

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

  @Test
  public void testIteration() {
    DictionaryWithParentSnapshot map = new DictionaryWithParentSnapshot();
    map.put("p1", RavenJValue.parse("5"));
    map.put("@id", RavenJValue.fromObject("c"));
    map.remove("@id");
    map.put("p2", RavenJValue.parse("5"));
    map.ensureSnapshot("snap!");

    DictionaryWithParentSnapshot snapshot = map.createSnapshot();
    snapshot.put("ch1", RavenJValue.parse("7"));
    snapshot.put("ch2", RavenJValue.parse("17"));

    Set<String> keys = new HashSet<>();
    for (Entry<String, RavenJToken> entry: snapshot) {
      keys.add(entry.getKey());
    }

    Set<String> expectedKeys = new HashSet<>(Arrays.asList("p1", "p2", "@id", "ch2", "ch1"));
    assertEquals(expectedKeys, keys);

  }
}
