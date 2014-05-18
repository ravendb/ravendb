package net.ravendb.client.extensions;

import static org.junit.Assert.assertEquals;

import net.ravendb.abstractions.data.Constants;
import net.ravendb.client.extensions.MultiDatabase;

import org.junit.Test;


public class MultiDatabaseTest {
  @Test
  public void testRootDbUrl() {
    assertEquals("http://localhost:8080", MultiDatabase.getRootDatabaseUrl("http://localhost:8080/"));
    assertEquals("http://localhost:8080", MultiDatabase.getRootDatabaseUrl("http://localhost:8080/databases/dbA"));
    assertEquals("http://localhost:8080", MultiDatabase.getRootDatabaseUrl("http://localhost:8080/databases/dbB/"));

    assertEquals(Constants.SYSTEM_DATABASE, MultiDatabase.getDatabaseName("http://localhost:8080/"));
    assertEquals("dbC", MultiDatabase.getDatabaseName("http://localhost:8080/databases/dbC"));
    assertEquals("dbCAA", MultiDatabase.getDatabaseName("http://localhost:8080/databases/dbCAA/"));
    assertEquals("db8", MultiDatabase.getDatabaseName("http://localhost:8123/databases/db8/docs/users%2Fmarcin"));




  }
}
