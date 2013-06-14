package raven.client.extensions;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import raven.abstractions.data.Constants;

public class MultiDatabaseTests {
  @Test
  public void testRootDbUrl() {
    assertEquals("http://localhost:8080", MultiDatabase.getRootDatabaseUrl("http://localhost:8080/"));
    assertEquals("http://localhost:8080", MultiDatabase.getRootDatabaseUrl("http://localhost:8080/databases/db1"));
    assertEquals("http://localhost:8080", MultiDatabase.getRootDatabaseUrl("http://localhost:8080/databases/db1/"));

    assertEquals(Constants.SYSTEM_DATABASE, MultiDatabase.getDatabaseName("http://localhost:8080/"));
    assertEquals("db1", MultiDatabase.getDatabaseName("http://localhost:8080/databases/db1"));
    assertEquals("db1AA", MultiDatabase.getDatabaseName("http://localhost:8080/databases/db1AA/"));
    assertEquals("db8", MultiDatabase.getDatabaseName("http://localhost:8123/databases/db8/docs/users%2Fmarcin"));




  }
}
