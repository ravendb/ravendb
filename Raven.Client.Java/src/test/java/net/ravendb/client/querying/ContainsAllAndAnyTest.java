package net.ravendb.client.querying;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.document.DocumentQueryCustomizationFactory;
import net.ravendb.client.document.DocumentStore;
import net.ravendb.samples.Developer;
import net.ravendb.samples.QDeveloper;
import net.ravendb.samples.Skill;

import org.junit.Test;


public class ContainsAllAndAnyTest extends RemoteClientTest {
  @Test
  public void shouldQueryWithContainsAny() throws Exception {

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {

      Developer developer1 = new Developer();
      developer1.setNick("ayende");
      developer1.setSkills(Arrays.asList(new Skill(".NET"), new Skill("PowerShell")));

      Developer developer2 = new Developer();
      developer2.setNick("marcin");
      developer2.setSkills(Arrays.asList(new Skill(".NET"), new Skill("Java")));

      QDeveloper d = QDeveloper.developer;

      try (IDocumentSession s = store.openSession()) {
        s.store(developer1);
        s.store(developer2);
        s.saveChanges();
        List<Developer> ret = s.query(Developer.class).where(d.skills.containsAny(Arrays.asList(new Skill("Python"),new Skill(".NET"))))
          .customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults(5 * 60 * 1000))
          .toList();

        assertEquals(2, ret.size());

        ret = s.query(Developer.class).where(d.skills.containsAny(Arrays.asList(new Skill("Python"),new Skill("Java"))))
          .customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults(5 * 60 * 1000))
          .toList();

        assertEquals(1, ret.size());
      }
    }
  }

  @Test
  public void shouldQueryWithContainsAll() throws Exception {

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {

      Developer developer1 = new Developer();
      developer1.setNick("ayende");
      developer1.setSkills(Arrays.asList(new Skill(".NET"), new Skill("PowerShell"), new Skill("RavenDB")));

      Developer developer2 = new Developer();
      developer2.setNick("marcin");
      developer2.setSkills(Arrays.asList(new Skill(".NET"), new Skill("Java")));

      QDeveloper d = QDeveloper.developer;

      try (IDocumentSession s = store.openSession()) {
        s.store(developer1);
        s.store(developer2);
        s.saveChanges();
        List<Developer> ret = s.query(Developer.class).where(d.skills.containsAll(Arrays.asList(new Skill(".NET"), new Skill("RavenDB"))))
          .customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults(5 * 60 * 1000))
          .toList();

        assertEquals(1, ret.size());

        ret = s.query(Developer.class).where(d.skills.containsAll(Arrays.asList(new Skill("Java"))))
          .customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults(5 * 60 * 1000))
          .toList();

        assertEquals(1, ret.size());
      }
    }
  }

  public final static class User {
    private String name;

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

  }
}
