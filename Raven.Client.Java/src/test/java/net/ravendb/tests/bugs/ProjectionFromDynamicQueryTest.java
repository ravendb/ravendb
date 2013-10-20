package net.ravendb.tests.bugs;

import static org.junit.Assert.assertEquals;

import net.ravendb.abstractions.json.linq.RavenJObject;
import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.document.DocumentStore;
import net.ravendb.client.linq.IRavenQueryable;
import net.ravendb.tests.bugs.QUser;

import org.junit.Test;


public class ProjectionFromDynamicQueryTest extends RemoteClientTest {

  @Test
  public void projectNameFromDynamicQueryUsingLucene() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        User user = new User();
        user.setName("Ayende");
        user.setEmail("Ayende@ayende.com");
        session.store(user);
        session.saveChanges();
      }

      try (IDocumentSession s = store.openSession()) {
        RavenJObject result = s.advanced().luceneQuery(User.class)
            .whereEquals("Name", "Ayende", true)
            .selectFields(RavenJObject.class, "Email")
            .first();

        assertEquals("Ayende@ayende.com", result.value(String.class, "Email"));
      }
    }
  }

  @Test
  public void projectNameFromDynamicQueryUsingLinq() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        User user = new User();
        user.setName("Ayende");
        user.setEmail("Ayende@ayende.com");
        session.store(user);
        session.saveChanges();
      }
      try (IDocumentSession session = store.openSession()) {
        QUser x = QUser.user;
        IRavenQueryable<String> result = session.query(User.class)
          .where(x.name.eq("Ayende"))
          .select(x.email);

        assertEquals("Ayende@ayende.com", result.first());
      }
    }
  }

  @Test
  public void projectNameFromDynamicQueryUsingLuceneUsingNestedObject() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        Person p = new Person();
        p.setName("Ayende");
        Address billingAddress = new Address();
        billingAddress.setCity("Bologna");
        p.setBillingAddress(billingAddress);

        session.store(p);
        session.saveChanges();
      }

      try (IDocumentSession s = store.openSession()) {
        RavenJObject result = s.advanced().luceneQuery(Person.class)
            .waitForNonStaleResults()
            .whereEquals("Name", "Ayende", true)
            .selectFields(RavenJObject.class, "BillingAddress")
            .first();

        assertEquals("Bologna", result.value(RavenJObject.class, "BillingAddress").value(String.class, "City"));
      }
    }
  }

  @Test
  public void projectNameFromDynamicQueryUsingLuceneUsingNestedProperty() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        Person p = new Person();
        p.setName("Ayende");
        Address billingAddress = new Address();
        billingAddress.setCity("Bologna");
        p.setBillingAddress(billingAddress);

        session.store(p);
        session.saveChanges();
      }

      try (IDocumentSession s = store.openSession()) {
        RavenJObject result = s.advanced().luceneQuery(Person.class)
            .whereEquals("Name", "Ayende", true)
            .selectFields(RavenJObject.class, "BillingAddress.City")
            .first();

        assertEquals("Bologna", result.value(String.class, "BillingAddress.City"));
      }
    }
  }

  @Test
  public void projectNameFromDynamicQueryUsingLuceneUsingNestedArray() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        Person p = new Person();
        p.setName("Ayende");
        Address billingAddress = new Address();
        billingAddress.setCity("Bologna");
        p.setBillingAddress(billingAddress);

        Address address1 = new Address();
        address1.setCity("Old York");
        p.setAddresses(new Address[] { address1 });

        session.store(p);
        session.saveChanges();

      }

      try (IDocumentSession s = store.openSession()) {
        RavenJObject result = s.advanced().luceneQuery(Person.class)
            .whereEquals("Name", "Ayende", true)
            .selectFields(RavenJObject.class, "Addresses[0].City")
            .first();

        assertEquals("Old York", result.value(String.class, "Addresses[0].City"));
      }
    }
  }

  public static class Person {
    private String name;
    private Address billingAddress;
    private Address[] addresses;
    public String getName() {
      return name;
    }
    public void setName(String name) {
      this.name = name;
    }
    public Address getBillingAddress() {
      return billingAddress;
    }
    public void setBillingAddress(Address billingAddress) {
      this.billingAddress = billingAddress;
    }
    public Address[] getAddresses() {
      return addresses;
    }
    public void setAddresses(Address[] addresses) {
      this.addresses = addresses;
    }



  }
  public static class Address {
    private String city;

    public String getCity() {
      return city;
    }

    public void setCity(String city) {
      this.city = city;
    }

  }

}
