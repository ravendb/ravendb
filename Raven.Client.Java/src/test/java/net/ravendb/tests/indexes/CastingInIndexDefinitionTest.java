package net.ravendb.tests.indexes;

import static org.junit.Assert.assertEquals;

import java.util.List;

import net.ravendb.abstractions.json.linq.RavenJObject;
import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.document.DocumentQueryCustomizationFactory;
import net.ravendb.client.document.DocumentStore;
import net.ravendb.client.indexes.AbstractIndexCreationTask;
import net.ravendb.tests.indexes.CastingInIndexDefinitionTest.Employees_CurrentCount.Result;

import org.junit.Test;


public class CastingInIndexDefinitionTest extends RemoteClientTest {

  @Test
  public void canCastValuesToString() throws Exception {

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      store.executeIndex(new Employees_CurrentCount());

      try (IDocumentSession session = store.openSession()) {
        Employee e1 = new Employee();
        e1.setId("employees/1");
        e1.setName("John");
        e1.setPayRate(10);
        session.store(e1);

        Employee e2 = new Employee();
        e2.setId("employees/2");
        e2.setName("Mary");
        e2.setPayRate(20);
        session.store(e2);

        Employee e3 = new Employee();
        e3.setId("employees/3");
        e3.setName("Sam");
        e3.setPayRate(30);
        session.store(e3);

        session.saveChanges();
      }

      // Make some changes
      try (IDocumentSession session = store.openSession()) {
        Employee employee1 = session.load(Employee.class, "employees/1");
        RavenJObject metadata1 = session.advanced().getMetadataFor(employee1);
        metadata1.add("test", "1");

        Employee employee2 = session.load(Employee.class, "employees/2");
        RavenJObject metadata2 = session.advanced().getMetadataFor(employee2);
        metadata2.add("test", "2");

        Employee employee3 = session.load(Employee.class, "employees/3");
        RavenJObject metadata3 = session.advanced().getMetadataFor(employee3);
        metadata3.add("test", "2");

        session.saveChanges();
      }

      // Query and check the results
      try (IDocumentSession session = store.openSession()) {
        List<Result> result = session.query(Employees_CurrentCount.Result.class, Employees_CurrentCount.class)
          .customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults())
          .toList();

        assertEquals(2, result.get(0).getCount());
      }

    }
  }

  public static class Employees_CurrentCount extends AbstractIndexCreationTask {
    public static class Result {
      private int count;

      public int getCount() {
        return count;
      }

      public void setCount(int count) {
        this.count = count;
      }

    }
    public Employees_CurrentCount() {
      map = "from employee in docs.employees " +
      		"let status = employee[\"@metadata\"] " +
      		"where status.Value<string>(\"test\") == \"2\" " +
      		"select new { Count = 1 }";
      reduce = "from result in results " +
      		"group result by 0 " +
      		"into g " +
      		"select new { Count = g.Sum(x => x.Count) };";
    }

  }


  public static class Employee {
    private String id;
    private String name;
    private double payRate;
    public String getId() {
      return id;
    }
    public void setId(String id) {
      this.id = id;
    }
    public String getName() {
      return name;
    }
    public void setName(String name) {
      this.name = name;
    }
    public double getPayRate() {
      return payRate;
    }
    public void setPayRate(double payRate) {
      this.payRate = payRate;
    }

  }

}
