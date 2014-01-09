package net.ravendb.tests.json;

import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.document.DocumentStore;

import org.junit.Test;


public class Webinar extends RemoteClientTest {

  public static class Person {
    private String name;
    private int age;
    private int id;

    // getters, setters

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public int getAge() {
      return age;
    }

    public int getId() {
      return id;
    }

    public void setId(int id) {
      this.id = id;
    }

    public void setAge(int age) {
      this.age = age;
    }

    public Person() {
      super();
    }

    public Person(String name, int age) {
      super();
      this.name = name;
      this.age = age;
    }

  }

  @Test
  public void can_store_entity() throws Exception {

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      int personId = 0;
      try (IDocumentSession session = store.openSession()) {
        Person person = new Person("Marcin", 26);
        session.store(person);
        personId = person.getId();
        session.saveChanges();
      }

      try (IDocumentSession session = store.openSession()) {
        Person person = session.load(Person.class, personId);
        person.setAge(30);
        session.saveChanges();
      }

    }
  }
}
