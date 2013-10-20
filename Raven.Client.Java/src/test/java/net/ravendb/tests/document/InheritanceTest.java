package net.ravendb.tests.document;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.UUID;

import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.document.DocumentStore;
import net.ravendb.client.document.TypeTagNameFinder;

import org.junit.Test;


public class InheritanceTest extends RemoteClientTest {

  @Test
  public void canStorePolymorphicTypesAsDocuments() throws Exception {

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {

      store.getConventions().setFindTypeTagName(new TypeTagNameFinder() {
        @Override
        public String find(Class< ? > clazz) {
          return IServer.class.isAssignableFrom(clazz) ? "Servers" : null;
        }
      });

      try (IDocumentSession session = store.openSession()) {
        WindowsServer windowsServer = new WindowsServer();
        windowsServer.setProductKey(UUID.randomUUID().toString());
        session.store(windowsServer);

        LinuxServer linuxServer = new LinuxServer();
        linuxServer.setKernelVersion("2.6.7");
        session.store(linuxServer);

        session.saveChanges();

        List<IServer> servers = session.advanced().luceneQuery(IServer.class).waitForNonStaleResults().toList();
        assertEquals(2, servers.size());
      }
    }
  }

  public static class LinuxServer implements IServer {
    private String id;
    private String kernelVersion;
    public String getId() {
      return id;
    }
    public void setId(String id) {
      this.id = id;
    }
    public String getKernelVersion() {
      return kernelVersion;
    }
    public void setKernelVersion(String kernelVersion) {
      this.kernelVersion = kernelVersion;
    }

    @Override
    public void start() {
      //empty by design
    }
  }

  public static class WindowsServer implements IServer {
    private String id;
    private String productKey;
    public String getId() {
      return id;
    }
    public void setId(String id) {
      this.id = id;
    }
    public String getProductKey() {
      return productKey;
    }
    public void setProductKey(String productKey) {
      this.productKey = productKey;
    }

    @Override
    public void start() {
    }
  }

  public static interface IServer {
    public void start();
  }
}
