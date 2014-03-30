package net.ravendb.tests.multiget;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;

import net.ravendb.abstractions.basic.Lazy;
import net.ravendb.abstractions.basic.Reference;
import net.ravendb.abstractions.closure.Action1;
import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.RavenQueryStatistics;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.document.DocumentQueryCustomizationFactory;
import net.ravendb.client.document.DocumentStore;
import net.ravendb.tests.bugs.transformresults.Answer;
import net.ravendb.tests.bugs.transformresults.AnswerEntity;
import net.ravendb.tests.bugs.transformresults.Answers_ByAnswerEntity;
import net.ravendb.tests.bugs.transformresults.ComplexValuesFromTransformResults;
import net.ravendb.tests.bugs.transformresults.QAnswer;
import net.ravendb.tests.linq.QUser;
import net.ravendb.tests.linq.User;

import org.junit.Test;


public class MultiGetQueriesTest extends RemoteClientTest {

  @Test
  public void unlessAccessedLazyQueriesAreNoOp() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        QUser x = QUser.user;
        session.query(User.class).where(x.name.eq("oren")).lazily();
        session.query(User.class).where(x.name.eq("ayende")).lazily();
        assertEquals(0, session.advanced().getNumberOfRequests());
      }
    }
  }

  @Test
  public void withPaging() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        User user1 = new User();
        user1.setName("oren");
        session.store(user1);
        session.store(new User());
        User user2 = new User();
        user2.setName("ayende");
        session.store(user2);
        session.store(new User());
        session.saveChanges();
      }
      try (IDocumentSession session = store.openSession()) {
        QUser x = QUser.user;
        Lazy<List<User>> result1 = session.query(User.class).where(x.age.eq(0)).skip(1).take(2).lazily();
        assertEquals(2, result1.getValue().size());
      }
    }
  }

  @Test
  public void canGetQueryStats() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        User user1 = new User();
        user1.setName("oren");
        session.store(user1);
        session.store(new User());
        User user2 = new User();
        user2.setName("ayende");
        session.store(user2);
        User user3 = new User();
        user3.setAge(3);
        session.store(user3);
        session.saveChanges();
      }
      try (IDocumentSession session = store.openSession()) {
        QUser x = QUser.user;
        Reference<RavenQueryStatistics> stats1 = new Reference<>();
        Lazy<List<User>> result1 = session.query(User.class).customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults())
            .statistics(stats1)
            .where(x.age.eq(0)).skip(1).take(2).lazily();

        Reference<RavenQueryStatistics> stats2 = new Reference<>();
        Lazy<List<User>> result2 = session.query(User.class).customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults())
            .statistics(stats2)
            .where(x.age.eq(3)).skip(1).take(2).lazily();
        assertEquals(2, result1.getValue().size());
        assertEquals(3, stats1.value.getTotalResults());

        assertEquals(0, result2.getValue().size());
        assertEquals(1, stats2.value.getTotalResults());
      }
    }
  }

  private List<User> users = null;

  @Test
  public void withQueuedActions() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        User user1 = new User();
        user1.setName("oren");
        session.store(user1);
        session.store(new User());
        User user2 = new User();
        user2.setName("ayende");
        session.store(user2);
        session.store(new User());
        session.saveChanges();
      }
      try (IDocumentSession session = store.openSession()) {
        QUser x = QUser.user;
        session.query(User.class).where(x.age.eq(0)).skip(1).take(2).lazily(new Action1<List<User>>() {

          @Override
          public void apply(List<User> first) {
            users = first;
          }
        });
        session.advanced().eagerly().executeAllPendingLazyOperations();
        assertEquals(2, users.size());
      }
    }
  }

  private User user = null;

  @Test
  public void withQueuedActions_Load() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        User user1 = new User();
        user1.setName("oren");
        session.store(user1);
        session.saveChanges();
      }
      try (IDocumentSession session = store.openSession()) {
        session.advanced().lazily().load(User.class, "users/1", new Action1<User>() {
          @Override
          public void apply(User first) {
            user = first;
          }
        });

        session.advanced().eagerly().executeAllPendingLazyOperations();
        assertNotNull(user);
      }
    }
  }

  @Test
  public void write_then_read_from_complex_entity_types_lazily() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      new Answers_ByAnswerEntity().execute(store);

      String answerId = ComplexValuesFromTransformResults.createEntities(store);
      QAnswer x = QAnswer.answer;

      try (IDocumentSession session = store.openSession()) {
        Lazy<List<AnswerEntity>> answerInfo = session.query(Answer.class, Answers_ByAnswerEntity.class).customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults())
            .where(x.id.eq(answerId))
            .as(AnswerEntity.class).lazily();
        assertEquals(1, answerInfo.getValue().size());
      }

    }
  }

  @Test
  public void lazyOperationsAreBatched() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        QUser x = QUser.user;
        Lazy<List<User>> result1 = session.query(User.class).where(x.name.eq("oren")).lazily();
        Lazy<List<User>> result2 = session.query(User.class).where(x.name.eq("ayende")).lazily();
        assertEquals(0, result2.getValue().size());
        assertEquals(1, session.advanced().getNumberOfRequests());
        assertEquals(0, result1.getValue().size());
        assertEquals(1, session.advanced().getNumberOfRequests());
      }
    }
  }

  @Test
  public void lazyMultiLoadOperationWouldBeInTheSession() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        User user1 = new User();
        user1.setName("oren");
        session.store(user1);
        session.store(new User());
        User user2 = new User();
        user2.setName("ayende");
        session.store(user2);
        session.store(new User());
        session.saveChanges();
      }
      QUser x = QUser.user;
      try (IDocumentSession session = store.openSession()) {
        session.query(User.class).customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults())
        .where(x.name.eq("oren"))
        .toList();
      }
      try (IDocumentSession session = store.openSession()) {
        Lazy<List<User>> result1 = session.query(User.class).where(x.name.eq("oren")).lazily();
        Lazy<List<User>> result2 = session.query(User.class).where(x.name.eq("ayende")).lazily();
        assertTrue(result2.getValue().size() > 0);
        assertEquals(1, session.advanced().getNumberOfRequests());

        assertTrue(result1.getValue().size() > 0);
        assertEquals(1, session.advanced().getNumberOfRequests());
      }
    }
  }

  @Test
  public void lazyWithProjection() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        User user1 = new User();
        user1.setName("oren");
        session.store(user1);
        User user2 = new User();
        user2.setName("ayende");
        session.store(user2);
        session.saveChanges();
      }
      QUser x = QUser.user;
      try (IDocumentSession session = store.openSession()) {
        session.query(User.class).customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults())
        .where(x.name.eq("oren"))
        .toList();
      }
      try (IDocumentSession session = store.openSession()) {
        Lazy<List<String>> result1 = session.query(User.class).where(x.name.eq("oren")).select(x.name).lazily();
        assertEquals("oren", result1.getValue().get(0));
      }
    }
  }

  @Test
  public void lazyWithProjection2() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        User user1 = new User();
        user1.setName("oren");
        session.store(user1);
        User user2 = new User();
        user2.setName("ayende");
        session.store(user2);
        session.saveChanges();
      }
      QUser x = QUser.user;
      try (IDocumentSession session = store.openSession()) {
        session.query(User.class).customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults())
        .where(x.name.eq("oren"))
        .toList();
      }
      try (IDocumentSession session = store.openSession()) {
        List<String> result1 = session.query(User.class).where(x.name.eq("oren")).select(x.name).toList();
        assertEquals("oren", result1.get(0));
      }
    }
  }

  @Test
  public void lazyMultiLoadOperationWouldBeInTheSession_WithNonStaleResponse() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        session.query(User.class).toList();

        User user1 = new User();
        user1.setName("oren");
        session.store(user1);
        User user2 = new User();
        user2.setName("ayende");
        session.store(user2);
        session.saveChanges();
      }
      QUser x = QUser.user;
      try (IDocumentSession session = store.openSession()) {
        Lazy<List<User>> result1 = session.query(User.class).customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults(5 * 60 * 1000))
            .where(x.name.eq("oren"))
            .lazily();
        Lazy<List<User>> result2 = session.query(User.class).customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults(5 * 60 * 1000))
            .where(x.name.eq("ayende"))
            .lazily();
        assertTrue(result2.getValue().size() > 0);
        assertEquals(1, session.advanced().getNumberOfRequests());
        assertTrue(result1.getValue().size() > 0);
        assertEquals(1, session.advanced().getNumberOfRequests());
      }
    }
  }

  @Test
  public void canGetStatisticsWithLazyQueryResults() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {

        User user1 = new User();
        user1.setName("oren");
        session.store(user1);
        User user2 = new User();
        user2.setName("ayende");
        session.store(user2);
        session.saveChanges();
      }
      QUser x = QUser.user;
      try (IDocumentSession session = store.openSession()) {
        session.query(User.class)
        .customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults())
        .where(x.name.eq("test"))
        .toList();
      }
      try (IDocumentSession session = store.openSession()) {
        Reference<RavenQueryStatistics> stats1 = new Reference<>();
        Lazy<List<User>> result1 = session.query(User.class).statistics(stats1).where(x.name.eq("oren")).lazily();
        Reference<RavenQueryStatistics> stats2 = new Reference<>();
        Lazy<List<User>> result2 = session.query(User.class).statistics(stats2).where(x.name.eq("ayende")).lazily();

        assertTrue(result2.getValue().size() > 0);
        assertEquals(1, session.advanced().getNumberOfRequests());
        assertTrue(result1.getValue().size() > 0);
        assertEquals(1, session.advanced().getNumberOfRequests());
      }
    }
  }
}
