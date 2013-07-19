package raven.client.util;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class InflectorTest {

  @Test
  public void testInflector() {
    testPluar("Users", "User");
    testPluar("tanimports", "tanimport");
    testPluar("fish", "fish");
    testSingualar("Person", "People");
    testPluar("tanimports", "tanimports");
    testPluar("money", "money");
    testSingualar("money", "money");
    testSingualar("sex", "sexes");
    testPluar("people", "person");
    testPluar("mice", "mouse");
    testSingualar("mouse", "mice");

  }

  private void testPluar(String expected, String input) {
    assertEquals(expected, Inflector.pluralize(input));
  }

  private void testSingualar(String expected, String input) {
    assertEquals(expected, Inflector.singularize(input));
  }

}

