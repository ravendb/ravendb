package net.ravendb.tests.indexes;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.document.DocumentQueryCustomizationFactory;
import net.ravendb.client.document.DocumentStore;
import net.ravendb.client.indexes.AbstractIndexCreationTask;
import net.ravendb.client.indexes.AbstractMultiMapIndexCreationTask;
import net.ravendb.tests.indexes.QBoostingDuringIndexingTest_User;
import net.ravendb.tests.indexes.QBoostingDuringIndexingTest_UserAndAccounts_Result;

import org.junit.Test;

import com.mysema.query.annotations.QueryEntity;


public class BoostingDuringIndexingTest extends RemoteClientTest {

  @QueryEntity
  public static class User {
    private String firstName;
    private String lastName;
    public String getFirstName() {
      return firstName;
    }
    public void setFirstName(String firstName) {
      this.firstName = firstName;
    }
    public String getLastName() {
      return lastName;
    }
    public void setLastName(String lastName) {
      this.lastName = lastName;
    }
  }

  @QueryEntity
  public static class Account {
    private String name;

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }
  }

  public static class UsersByName extends AbstractIndexCreationTask {
    public UsersByName() {
      map = "from user in docs.users select new { FirstName = user.FirstName.Boost(3), user.LastName}";
    }
  }

  public static class UserAndAccounts extends AbstractMultiMapIndexCreationTask {
    @QueryEntity
    public static class Result {
      private String name;

      public String getName() {
        return name;
      }

      public void setName(String name) {
        this.name = name;
      }
    }

    public UserAndAccounts() {
      addMap("from user in docs.users select new { Name = user.FirstName} ");
      addMap("from account in docs.accounts select new {account.Name}.Boost(3)");
    }
  }

  @Test
  public void canBoostFullDocument() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {

      new UserAndAccounts().execute(store);

      try (IDocumentSession session = store.openSession()) {
        User user = new User();
        user.setFirstName("Oren");
        session.store(user);

        Account account = new Account();
        account.setName("Oren");
        session.store(account);
        session.saveChanges();
      }

      QBoostingDuringIndexingTest_UserAndAccounts_Result x = QBoostingDuringIndexingTest_UserAndAccounts_Result.result;

      try (IDocumentSession session = store.openSession()) {
        List<Object> results = session.query(UserAndAccounts.Result.class, UserAndAccounts.class)
        .customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults())
        .where(x.name.eq("Oren"))
        .as(Object.class).toList();

        assertEquals(2, results.size());
        assertTrue(results.get(0) instanceof Account);
        assertTrue(results.get(1) instanceof User);

      }
    }
  }

  @Test
  public void canGetBoostedValues() throws Exception {

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {

      new UsersByName().execute(store);

      try (IDocumentSession session = store.openSession()) {
        User user = new User();
        user.setFirstName("Oren");
        user.setLastName("Eini");
        session.store(user);

        User user2 = new User();
        user2.setFirstName("Ayende");
        user2.setLastName("Rahien");
        session.store(user2);

        session.saveChanges();
      }
      QBoostingDuringIndexingTest_User x = QBoostingDuringIndexingTest_User.user;

      try (IDocumentSession session = store.openSession()) {
        List<User> users = session.query(User.class, UsersByName.class)
          .customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults())
          .where(x.firstName.eq("Ayende").or(x.lastName.eq("Eini")))
          .toList();


        assertEquals("Ayende", users.get(0).getFirstName());
        assertEquals("Oren", users.get(1).getFirstName());
      }
    }
  }


}
