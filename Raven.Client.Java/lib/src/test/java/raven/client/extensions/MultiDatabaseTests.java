package raven.client.extensions;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class MultiDatabaseTests {
  @Test
  public void testRootDbUrl() {
    assertEquals("http://localhost:8080", MultiDatabase.getRootDatabaseUrl("http://localhost:8080/"));
    assertEquals("http://localhost:8080", MultiDatabase.getRootDatabaseUrl("http://localhost:8080/databases/db1"));
    assertEquals("http://localhost:8080", MultiDatabase.getRootDatabaseUrl("http://localhost:8080/databases/db1/"));
  }
}
