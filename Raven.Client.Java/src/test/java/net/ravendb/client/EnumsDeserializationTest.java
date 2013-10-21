package net.ravendb.client;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import net.ravendb.abstractions.extensions.JsonExtensions;
import net.ravendb.abstractions.indexing.IndexLockMode;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.junit.Test;


public class EnumsDeserializationTest {

  public static class LockInfo {
    private IndexLockMode lockMode;

    public IndexLockMode getLockMode() {
      return lockMode;
    }

    public void setLockMode(IndexLockMode lockMode) {
      this.lockMode = lockMode;
    }
  }

  @Test
  public void test() throws JsonParseException, JsonMappingException, IOException {
    LockInfo lockInfo = JsonExtensions.createDefaultJsonSerializer().readValue("{\"LockMode\" : \"LockedIgnore\"  }", LockInfo.class);
    assertEquals(IndexLockMode.LOCKED_IGNORE, lockInfo.getLockMode());
  }
}
