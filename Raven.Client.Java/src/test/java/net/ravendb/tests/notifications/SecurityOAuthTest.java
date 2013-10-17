package net.ravendb.tests.notifications;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import net.ravendb.abstractions.closure.Action1;
import net.ravendb.abstractions.data.DocumentChangeNotification;
import net.ravendb.abstractions.data.DocumentChangeTypes;
import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.document.DocumentStore;
import net.ravendb.client.document.FailoverBehavior;
import net.ravendb.client.document.FailoverBehaviorSet;
import net.ravendb.client.utils.Observers;

import org.junit.Ignore;
import org.junit.Test;



public class SecurityOAuthTest extends RemoteClientTest {

  @Test
  @Ignore("Cant run this test in .NET on host different than localhost")
  public void withOAuthOnSystemDatabase() throws Exception {
    stopServerAfter();
    startServerWithOAuth(DEFAULT_SERVER_PORT_1);
    try (IDocumentStore store = new DocumentStore(getDefaultUrl()).withApiKey("java/6B4G51NrO0P")) {
      store.getConventions().setFailoverBehavior(new FailoverBehaviorSet(FailoverBehavior.FAIL_IMMEDIATELY));
      store.initialize();

      final BlockingQueue<DocumentChangeNotification> list = new ArrayBlockingQueue<>(20);

      store.changes().forDocument("items/1").subscribe(Observers.create(new Action1<DocumentChangeNotification>() {
        @Override
        public void apply(DocumentChangeNotification value) {
          list.add(value);
        }
      }));

      try (IDocumentSession session = store.openSession()) {
        session.store(new ClientServerTest.Item(), "items/1");
        session.saveChanges();
      }

      DocumentChangeNotification documentChangeNotification = list.poll(15, TimeUnit.SECONDS);
      assertNotNull(documentChangeNotification);

      assertEquals("items/1", documentChangeNotification.getId());
      assertEquals(documentChangeNotification.getType(), DocumentChangeTypes.PUT);

    }
//FIXME: other tests - wait for fix in .net
  }
}
