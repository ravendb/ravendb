package raven.tests.notifications;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.Ignore;
import org.junit.Test;

import raven.abstractions.closure.Action1;
import raven.abstractions.data.DocumentChangeNotification;
import raven.abstractions.data.DocumentChangeTypes;
import raven.client.IDocumentSession;
import raven.client.IDocumentStore;
import raven.client.RemoteClientTest;
import raven.client.document.DocumentStore;
import raven.client.document.FailoverBehavior;
import raven.client.document.FailoverBehaviorSet;
import raven.client.utils.Observers;


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
//FIXME: other tests
  }
}
