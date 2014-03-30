package net.ravendb.client.connection.profiling;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Set;

import net.ravendb.abstractions.closure.Action1;
import net.ravendb.abstractions.closure.Function1;
import net.ravendb.client.connection.profiling.ConcurrentLruSet;

import org.junit.Test;


public class ConcurrentLruLSetTest {
  @Test
  public void testCleanOnEmpty() {
    final Set<String> dropped = new HashSet<>();

    ConcurrentLruSet<String> set1 = new ConcurrentLruSet<>(5, new Action1<String>() {
      @Override
      public void apply(String value) {
        dropped.add(value);
      }
    });

    set1.clearHalf();
  }

  @Test
  public void test() {

    final Set<String> dropped = new HashSet<>();

    ConcurrentLruSet<String> set1 = new ConcurrentLruSet<>(5, new Action1<String>() {
      @Override
      public void apply(String value) {
        dropped.add(value);
      }
    });

    set1.push("Item #1");
    set1.push("Item #2");
    set1.push("Item #3");
    set1.push("Item #4");
    set1.push("Item #5");

    assertTrue(dropped.isEmpty());

    set1.push("Item #6");
    assertTrue(dropped.iterator().next().equals("Item #1"));

    String string = set1.firstOrDefault(new Function1<String, Boolean>() {
      @Override
      public Boolean apply(String input) {
        return "Item #4".equals(input);
      }
    });

    assertNotNull(string);

    set1.clearHalf();
    assertEquals(3, dropped.size());
    set1.clearHalf();
    assertEquals(4, dropped.size());
    set1.clearHalf();
    assertEquals(5, dropped.size());
    set1.clearHalf();
    assertEquals(5, dropped.size());


  }
}
