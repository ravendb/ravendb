package net.ravendb.tests.linq;

import static org.junit.Assert.assertFalse;

import java.util.List;

import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.document.DocumentQueryCustomizationFactory;
import net.ravendb.client.document.DocumentStore;
import net.ravendb.client.indexes.AbstractIndexCreationTask;
import net.ravendb.tests.linq.QFlagsEnumTest_MyIndex_Result;

import org.junit.Test;

import com.mysema.query.annotations.QueryEntity;


public class FlagsEnumTest extends RemoteClientTest {

  public static enum CustomEnum {
    NONE,
    ONE,
    TWO;
  }

  public static class Entity {
    private String id;
    private String name;
    private CustomEnum[] status;
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
    public CustomEnum[] getStatus() {
      return status;
    }
    public void setStatus(CustomEnum[] status) {
      this.status = status;
    }
  }


  public static class MyIndex extends AbstractIndexCreationTask {
    @QueryEntity
    public static class Result {
      private CustomEnum status;

      public CustomEnum getStatus() {
        return status;
      }

      public void setStatus(CustomEnum status) {
        this.status = status;
      }
    }

    public MyIndex() {
      map = "from entity in docs.entities select new { Status = entity.Status } ";
    }
  }

  @Test
  public void canQueryUsingEnum() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      new MyIndex().execute(store);
      try (IDocumentSession session = store.openSession()) {
        Entity entity = new Entity();
        entity.setName("birsey");
        entity.setStatus(new CustomEnum[] { CustomEnum.ONE, CustomEnum.TWO });

        session.store(entity);
        session.saveChanges();
      }
      try (IDocumentSession session = store.openSession()) {
        QFlagsEnumTest_MyIndex_Result x = QFlagsEnumTest_MyIndex_Result.result;
        List<Entity> results = session.query(MyIndex.Result.class, MyIndex.class)
          .customize(new DocumentQueryCustomizationFactory().waitForNonStaleResultsAsOfLastWrite())
          .where(x.status.eq(CustomEnum.TWO))
          .as(Entity.class)
          .toList();

        assertFalse(results.isEmpty());
      }
    }
  }
}

