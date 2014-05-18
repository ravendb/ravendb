package net.ravendb.client.document;

import static org.junit.Assert.fail;

import net.ravendb.client.document.DocumentStore;

import org.junit.Assert;
import org.junit.Test;


public class ConnectionStringsTest {

  @Test
  public void checkUrl() throws Exception {
    try (DocumentStore store = new DocumentStore()) {
      store.parseConnectionString("Url=http://localhost:8079/;");

      Assert.assertEquals("http://localhost:8079", store.getUrl());
      Assert.assertEquals("http://localhost:8079", store.getIdentifier());
      Assert.assertNull(store.getDefaultDatabase());
    } catch (Exception e) {
      throw e;
    }
  }

  @Test
  public void checkIllegalConnstrings() throws Exception {
    try (DocumentStore store = new DocumentStore()) {
      try {
        store.parseConnectionString("");
        fail();
      } catch (Exception e) {
        //ok
      }
    } catch (Exception e) {
      throw e;
    }
  }

  @Test
  public void checkUrlAndRmid() throws Exception {
    try (DocumentStore store = new DocumentStore()) {
      store.parseConnectionString("Url=http://localhost:8079/;");

      Assert.assertEquals("http://localhost:8079", store.getUrl());
      Assert.assertEquals("http://localhost:8079", store.getIdentifier());
      Assert.assertNull(store.getDefaultDatabase());
    } catch (Exception e) {
      throw e;
    }
  }

  @Test
  public void checkDefaultdb() throws Exception {
    try (DocumentStore store = new DocumentStore()) {
      store.parseConnectionString("DefaultDatabase=DevMachine;");

      Assert.assertNull(store.getUrl());
      Assert.assertNull(store.getIdentifier());
      Assert.assertEquals("DevMachine", store.getDefaultDatabase());
    } catch (Exception e) {
      throw e;
    }
  }

  @Test
  public void checkUrlAndDefaultdb() throws Exception {
    try (DocumentStore store = new DocumentStore()) {
      store.parseConnectionString("Url=http://localhost:8079/;DefaultDatabase=DevMachine;");

      Assert.assertEquals("http://localhost:8079", store.getUrl());
      Assert.assertEquals("http://localhost:8079 (DB: DevMachine)", store.getIdentifier());
      Assert.assertEquals("DevMachine", store.getDefaultDatabase());
    } catch (Exception e) {
      throw e;
    }
  }

  @Test
  public void canWorkWithDefaultDb() throws Exception {
    try (DocumentStore store = new DocumentStore()) {
      store.parseConnectionString("Url=http://localhost:8079/;DefaultDatabase=DevMachine;");

      Assert.assertEquals("http://localhost:8079", store.getUrl());
      Assert.assertEquals("http://localhost:8079 (DB: DevMachine)", store.getIdentifier());
      Assert.assertEquals("DevMachine", store.getDefaultDatabase());
    } catch (Exception e) {
      throw e;
    }
  }

  @Test
  public void canGetApiKey() throws Exception {
    try (DocumentStore store = new DocumentStore()) {
      store.parseConnectionString("Url=http://localhost:8079/;ApiKey=d5723e19-92ad-4531-adad-8611e6e05c8a;");

      Assert.assertEquals("d5723e19-92ad-4531-adad-8611e6e05c8a", store.getApiKey());
    } catch (Exception e) {
      throw e;
    }
  }



}
