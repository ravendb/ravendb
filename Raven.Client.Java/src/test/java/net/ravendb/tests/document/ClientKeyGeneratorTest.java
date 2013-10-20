package net.ravendb.tests.document;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.connection.IDatabaseCommands;
import net.ravendb.client.document.DocumentKeyGenerator;
import net.ravendb.client.document.DocumentStore;
import net.ravendb.client.document.MultiTypeHiLoKeyGenerator;

import org.junit.Test;


public class ClientKeyGeneratorTest extends RemoteClientTest {
  @Test
  public void idIsSetFromGeneratorOnStore() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        Company company = new Company();
        session.store(company);

        assertEquals("companies/1", company.getId());
      }
    }
  }

  @Test
  public void differentTypesWillHaveDifferentIdGenerators() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        Company company = new Company();
        session.store(company);

        Contact contact = new Contact();
        session.store(contact);

        assertEquals("companies/1", company.getId());
        assertEquals("contacts/1", contact.getId());
      }
    }
  }

  @Test
  public void whenDocumentAlreadyExists_Can_Still_Generate_Values() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {

      final MultiTypeHiLoKeyGenerator mk = new MultiTypeHiLoKeyGenerator(5);
      store.getConventions().setDocumentKeyGenerator(new DocumentKeyGenerator() {
        @Override
        public String generate(String dbName, IDatabaseCommands cmd, Object o) {
          return mk.generateDocumentKey(cmd, store.getConventions(), o);
        }
      });

      try (IDocumentSession session = store.openSession()) {
        Company company = new Company();
        session.store(company);
        Contact contact = new Contact();
        session.store(contact);

        assertEquals("companies/1", company.getId());
        assertEquals("contacts/1", contact.getId());
      }

      final MultiTypeHiLoKeyGenerator mk2 = new MultiTypeHiLoKeyGenerator(5);
      store.getConventions().setDocumentKeyGenerator(new DocumentKeyGenerator() {
        @Override
        public String generate(String dbName, IDatabaseCommands cmd, Object o) {
          return mk2.generateDocumentKey(cmd, store.getConventions(), o);
        }
      });

      try (IDocumentSession session = store.openSession()) {
        Company company = new Company();
        session.store(company);
        Contact contact = new Contact();
        session.store(contact);

        assertEquals("companies/6", company.getId());
        assertEquals("contacts/6", contact.getId());
      }
    }
  }

  @Test
  public void doesNotLoseValuesWhenHighIsOver() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      final MultiTypeHiLoKeyGenerator mk = new MultiTypeHiLoKeyGenerator(5);

      for (int i = 0; i < 15; i++) {
       assertEquals("companies/" + (i+1), mk.generateDocumentKey(store.getDatabaseCommands(), store.getConventions(), new Company()));
      }
    }
  }

  @Test
  public void idIsKeptFromGeneratorOnSaveChanges() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        Company company = new Company();
        session.store(company);
        session.saveChanges();

        assertEquals("companies/1", company.getId());

      }
    }
  }

  @Test
  public void noIdIsSetAndSoIdIsNullAfterStore() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      store.getConventions().setDocumentKeyGenerator(new DocumentKeyGenerator() {
        @Override
        public String generate(String dbName, IDatabaseCommands dbCommands, Object entity) {
          return null;
        }
      });

      try (IDocumentSession session = store.openSession()) {
        Company company = new Company();
        session.store(company);

        assertNull(company.getId());
      }
    }
  }

  @Test
  public void noIdIsSetAndSoIdIsSetAfterSaveChanges() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {

      store.getConventions().setDocumentKeyGenerator(new DocumentKeyGenerator() {
        @Override
        public String generate(String dbName, IDatabaseCommands dbCommands, Object entity) {
          return null;
        }
      });

      try (IDocumentSession session = store.openSession()) {
        Company company = new Company();
        session.store(company);
        session.saveChanges();

        assertNotNull(company.getId());
      }
    }
  }
}
