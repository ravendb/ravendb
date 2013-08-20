package raven.tests.bugs.indexing;

import static org.junit.Assert.assertEquals;

import java.util.EnumSet;
import java.util.List;

import org.junit.Test;

import raven.abstractions.data.AggregationOperation;
import raven.client.IDocumentSession;
import raven.client.IDocumentStore;
import raven.client.RemoteClientTest;
import raven.client.document.DocumentStore;

public class WillGroupValuesUsingComplexValuesTest extends RemoteClientTest {
  @Test
  public void canGroupByComplexObject() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession s = store.openSession()) {
        s.store(new User("Oren", new Address("New York", "Broadway")));
        s.store(new User("Eini", new Address("Halom", "Silk")));
        s.store(new User("Rahien", new Address("Halom", "Silk")));
        s.store(new User("Ayende", new Address("New York", "Broadway")));

        s.saveChanges();
      }

      try (IDocumentSession session = store.openSession()) {
        List<GroupResult> objects = session.advanced().luceneQuery(GroupResult.class, "dynamic/Users")
        .groupBy(EnumSet.of(AggregationOperation.COUNT), "Address")
        .orderBy("-Address.City")
        .waitForNonStaleResults(60 * 1000)
        .toList();


        assertEquals(2, objects.size());
        assertEquals(2, objects.get(0).getCount());
        assertEquals("New York", objects.get(0).getAddress().getCity());

        assertEquals(2, objects.get(1).getCount());
        assertEquals("Halom", objects.get(1).getAddress().getCity());

      }
    }
  }

  public static class GroupResult {
    private int count;
    private Address address;
    public int getCount() {
      return count;
    }
    public void setCount(int count) {
      this.count = count;
    }
    public Address getAddress() {
      return address;
    }
    public void setAddress(Address address) {
      this.address = address;
    }

  }

  public static class User {
    private String name;
    private Address address;
    public Address getAddress() {
      return address;
    }
    public void setAddress(Address address) {
      this.address = address;
    }
    public String getName() {
      return name;
    }
    public void setName(String name) {
      this.name = name;
    }
    public User() {
      super();
    }
    public User(String name, Address address) {
      super();
      this.name = name;
      this.address = address;
    }

  }

  public static class Address {
    private String city;
    private String street;

    public Address() {
      super();
    }
    public Address(String city, String street) {
      super();
      this.city = city;
      this.street = street;
    }
    public String getCity() {
      return city;
    }
    public void setCity(String city) {
      this.city = city;
    }
    public String getStreet() {
      return street;
    }
    public void setStreet(String street) {
      this.street = street;
    }

  }

}
