package net.ravendb.client.auth;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import net.ravendb.abstractions.exceptions.HttpOperationException;
import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.document.DocumentStore;
import net.ravendb.tests.bugs.User;

import org.apache.http.HttpStatus;
import org.junit.Test;

import com.google.common.base.Throwables;



public class OAuthTest extends RemoteClientTest  {
  @Test
  public void throws_unauthorized_when_api_key_is_missing() throws Exception {
    stopServerAfter();
    startServerWithOAuth(DEFAULT_SERVER_PORT_1);

    try {
      try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
        try (IDocumentSession session = store.openSession()) {
          User user = new User();
          user.setName("Ayende");
          session.store(user);
          session.saveChanges();
        }
      }
      fail();
    } catch (RuntimeException e) {
      Throwable rootCause = Throwables.getRootCause(e);
      HttpOperationException exp = (HttpOperationException) rootCause;
      assertEquals(HttpStatus.SC_UNAUTHORIZED, exp.getStatusCode());
    }
  }

  @Test
  public void throws_unauthorized_when_api_key_is_invalid() throws Exception {
    stopServerAfter();
    startServerWithOAuth(DEFAULT_SERVER_PORT_1);

    try {
      try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).withApiKey("java/INVALID").initialize()) {
        try (IDocumentSession session = store.openSession()) {
          User user = new User();
          user.setName("Ayende");
          session.store(user);
          session.saveChanges();
        }
      }
      fail();
    } catch (RuntimeException e) {
      Throwable rootCause = Throwables.getRootCause(e);
      if (rootCause instanceof HttpOperationException) {
        assertEquals(HttpStatus.SC_SERVICE_UNAVAILABLE, ((HttpOperationException) rootCause).getStatusCode());
      } else {
        throw e;
      }
    }
  }

  @Test
  public void throws_unauthorized_when_api_key_is_invalid2() throws Exception {
    stopServerAfter();
    startServerWithOAuth(DEFAULT_SERVER_PORT_1);

    try {
      try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).withApiKey("ja3va/INVALID").initialize()) {
        try (IDocumentSession session = store.openSession()) {
          User user = new User();
          user.setName("Ayende");
          session.store(user);
          session.saveChanges();
        }
      }
      fail();
    } catch (RuntimeException e) {
      Throwable rootCause = Throwables.getRootCause(e);
      if (rootCause instanceof HttpOperationException) {
        assertEquals(HttpStatus.SC_SERVICE_UNAVAILABLE, ((HttpOperationException) rootCause).getStatusCode());
      } else {
        throw e;
      }
    }
  }

  @Test
  public void can_use_oauth() throws Exception {
    stopServerAfter();
    startServerWithOAuth(DEFAULT_SERVER_PORT_1);

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).withApiKey("java/6B4G51NrO0P").initialize()) {
      try (IDocumentSession session = store.openSession()) {
        User user = new User();
        user.setName("Ayende");
        session.store(user);
        session.saveChanges();
      }
    }
  }

}
