package net.ravendb.tests.multiget;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import net.ravendb.abstractions.basic.Lazy;
import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.document.DocumentStore;
import net.ravendb.tests.bugs.User;

import org.junit.Test;


public class MultiGetMultiGetTest extends RemoteClientTest {

  @Test
  public void multiGetShouldBehaveTheSameForLazyAndNotLazy() throws Exception {

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        User[] result1 = session.load(User.class, "users/1", "users/2");
        Lazy<User[]> result2 = session.advanced().lazily().load(User.class, "users/3", "users/4");
        assertArrayEquals(new User[2], result1);
        assertArrayEquals(new User[2], result2.getValue());
      }
    }
  }

  @Test
  public void unlessAccessedLazyOperationsAreNoOp() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        session.advanced().lazily().load(User.class, "users/1", "users/2");
        session.advanced().lazily().load(User.class, "users/3", "users/4");

        assertEquals(0, session.advanced().getNumberOfRequests());
      }
    }
  }

  @Test
  public void lazyOperationsAreBatched() throws Exception {

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        Lazy<User[]> result1 = session.advanced().lazily().load(User.class, "users/1", "users/2");
        Lazy<User[]> result2 = session.advanced().lazily().load(User.class, "users/3", "users/4");

        assertArrayEquals(new User[2], result2.getValue());
        assertEquals(1, session.advanced().getNumberOfRequests());

        assertArrayEquals(new User[2], result1.getValue());
        assertEquals(1, session.advanced().getNumberOfRequests());
      }
    }
  }

  @Test
  public void lazyMultiLoadOperationWouldBeInTheSession() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        session.store(new User());
        session.store(new User());
        session.store(new User());
        session.store(new User());
        session.saveChanges();
      }

      try (IDocumentSession session = store.openSession()) {
        Lazy<User[]> result1 = session.advanced().lazily().load(User.class, "users/1", "users/2");
        Lazy<User[]> result2 = session.advanced().lazily().load(User.class, "users/3", "users/4");

        User[] a = result2.getValue();
        assertEquals(1, session.advanced().getNumberOfRequests());

        User[] b = result1.getValue();
        assertEquals(1, session.advanced().getNumberOfRequests());

        for (User user : a) {
          assertNotNull(session.advanced().getMetadataFor(user));
        }

        for (User user : b) {
          assertNotNull(session.advanced().getMetadataFor(user));
        }
      }
    }
  }

  @Test
  public void lazyLoadOperationWillHandleIncludes() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        User user2 = new User();
        user2.setName("users/2");
        User user4 = new User();
        user4.setName("users/4");
        session.store(user2);
        session.store(new User());
        session.store(user4);
        session.store(new User());
        session.saveChanges();
      }

      try (IDocumentSession session = store.openSession()) {
        Lazy<User> result1 = session.advanced().lazily().include("Name").load(User.class, "users/1");
        Lazy<User> result2 = session.advanced().lazily().include("Name").load(User.class, "users/3");
        assertNotNull(result1.getValue());
        assertNotNull(result2.getValue());

        assertEquals(1, session.advanced().getNumberOfRequests());
        assertEquals(1, session.advanced().getNumberOfRequests());
        assertTrue(session.advanced().isLoaded("users/2"));
        assertTrue(session.advanced().isLoaded("users/4"));
      }
    }
  }


}
