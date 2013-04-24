package raven.client;

import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import raven.client.json.RavenJObject;
import raven.client.json.lang.JsonWriterException;

public class RavenJObjectsTest {

  static class Person {
    private String name;
    private String surname;
    private int[] types;


    public int[] getTypes() {
      return types;
    }
    public void setTypes(int[] types) {
      this.types = types;
    }
    public String getName() {
      return name;
    }
    public void setName(String name) {
      this.name = name;
    }
    public String getSurname() {
      return surname;
    }
    public void setSurname(String surname) {
      this.surname = surname;
    }

  }

  @Test
  public void testRavenJObjectFromObject() throws JsonWriterException {
    Person person1 = new Person();
    person1.setName("Joe");
    person1.setSurname("Doe");
    person1.setTypes(new int[] { 1,2,3,4,5 });

    RavenJObject ravenJObject = RavenJObject.fromObject(person1);
    assertNotNull(ravenJObject);
    System.err.println(ravenJObject.getType());

  }
}
