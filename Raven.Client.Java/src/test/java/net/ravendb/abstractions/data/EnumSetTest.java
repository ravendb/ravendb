package net.ravendb.abstractions.data;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import net.ravendb.client.document.FailoverBehavior;
import net.ravendb.client.document.FailoverBehaviorSet;

import org.junit.Test;



public class EnumSetTest {
  @Test
  public void testFailverBehaviour() {
    FailoverBehaviorSet set = new FailoverBehaviorSet();
    assertTrue(set.contains(FailoverBehavior.FAIL_IMMEDIATELY));
    assertFalse(set.contains(FailoverBehavior.ALLOW_READS_FROM_SECONDARIES));
    assertFalse(set.contains(FailoverBehavior.ALLOW_READS_FROM_SECONDARIES_AND_WRITES_TO_SECONDARIES));
    assertFalse(set.contains(FailoverBehavior.READ_FROM_ALL_SERVERS));

    set.add(FailoverBehavior.READ_FROM_ALL_SERVERS);

    assertFalse(set.contains(FailoverBehavior.FAIL_IMMEDIATELY));
    assertFalse(set.contains(FailoverBehavior.ALLOW_READS_FROM_SECONDARIES));
    assertFalse(set.contains(FailoverBehavior.ALLOW_READS_FROM_SECONDARIES_AND_WRITES_TO_SECONDARIES));
    assertTrue(set.contains(FailoverBehavior.READ_FROM_ALL_SERVERS));

    set.add(FailoverBehavior.ALLOW_READS_FROM_SECONDARIES_AND_WRITES_TO_SECONDARIES);

    assertFalse(set.contains(FailoverBehavior.FAIL_IMMEDIATELY));
    assertTrue(set.contains(FailoverBehavior.ALLOW_READS_FROM_SECONDARIES));
    assertTrue(set.contains(FailoverBehavior.ALLOW_READS_FROM_SECONDARIES_AND_WRITES_TO_SECONDARIES));
    assertTrue(set.contains(FailoverBehavior.READ_FROM_ALL_SERVERS));

    set.remove(FailoverBehavior.ALLOW_READS_FROM_SECONDARIES);

    assertFalse(set.contains(FailoverBehavior.FAIL_IMMEDIATELY));
    assertFalse(set.contains(FailoverBehavior.ALLOW_READS_FROM_SECONDARIES));
    assertFalse(set.contains(FailoverBehavior.ALLOW_READS_FROM_SECONDARIES_AND_WRITES_TO_SECONDARIES));
    assertTrue(set.contains(FailoverBehavior.READ_FROM_ALL_SERVERS));

  }
}
