package net.ravendb.tests.linq;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.document.DocumentQueryCustomizationFactory;
import net.ravendb.client.document.DocumentStore;
import net.ravendb.client.indexes.AbstractIndexCreationTask;

import org.junit.Test;


public class SelectManyShouldWorkTest extends RemoteClientTest {


  public void insertSampleData(IDocumentStore store) throws Exception {

    try (IDocumentSession session = store.openSession()) {
      DroneStateSnapshot d1 = new DroneStateSnapshot();
      d1.setClickActions(Arrays.asList(
          new ClickAction("contact/1", "creative/1"),
          new ClickAction("contact/2", "creative/1")
          ));
      session.store(d1);

      DroneStateSnapshot d2 = new DroneStateSnapshot();
      d2.setClickActions(Arrays.asList(
          new ClickAction("contact/100", "creative/1"),
          new ClickAction("contact/200", "creative/1")
          ));
      session.store(d2);

      DroneStateSnapshot d3 = new DroneStateSnapshot();
      d3.setClickActions(Arrays.asList(
          new ClickAction("contact/1000", "creative/2"),
          new ClickAction("contact/2000", "creative/2")
          ));
      session.store(d3);

      DroneStateSnapshot d4 = new DroneStateSnapshot();
      d4.setClickActions(Arrays.asList(
          new ClickAction("contact/4000", "creative/2"),
          new ClickAction("contact/5000", "creative/2")
          ));
      session.store(d4);

      session.saveChanges();
    }

  }

  @Test
  public void selectMany1_Works() throws Exception {
    assertAgainstIndex(Creatives_ClickActions_1.class);
  }

  @Test
  public void selectMany2_ShouldWork() throws Exception {
    assertAgainstIndex(Creatives_ClickActions_2.class);
  }

  public <TIndex extends AbstractIndexCreationTask> void assertAgainstIndex(Class<TIndex> indexClass) throws Exception {

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      insertSampleData(store);
      indexClass.newInstance().execute(store);
      try (IDocumentSession session = store.openSession()) {
        List<ReduceResult> result = session.query(ReduceResult.class, indexClass)
            .customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults())
            .toList();
        assertEquals(2, result.size());
        assertEquals("creative/1", result.get(0).getCreativeId());
        assertEquals("creative/2", result.get(result.size() - 1).getCreativeId());
      }
    }
  }

  public static class DroneStateSnapshot {
    private List<ClickAction> clickActions;

    public List<ClickAction> getClickActions() {
      return clickActions;
    }

    public void setClickActions(List<ClickAction> clickActions) {
      this.clickActions = clickActions;
    }
  }

  public static class ClickAction {
    private String contactId;
    private String creativeId;
    private Date date;


    public ClickAction() {
      super();
    }
    public ClickAction(String contactId, String creativeId) {
      super();
      this.contactId = contactId;
      this.creativeId = creativeId;
    }
    public String getContactId() {
      return contactId;
    }
    public void setContactId(String contactId) {
      this.contactId = contactId;
    }
    public String getCreativeId() {
      return creativeId;
    }
    public void setCreativeId(String creativeId) {
      this.creativeId = creativeId;
    }
    public Date getDate() {
      return date;
    }
    public void setDate(Date date) {
      this.date = date;
    }
  }

  public static class ReduceResult {
    private String creativeId;
    private String[] clickedBy;
    public String getCreativeId() {
      return creativeId;
    }
    public void setCreativeId(String creativeId) {
      this.creativeId = creativeId;
    }
    public String[] getClickedBy() {
      return clickedBy;
    }
    public void setClickedBy(String[] clickedBy) {
      this.clickedBy = clickedBy;
    }

  }

  public static class Creatives_ClickActions_1 extends AbstractIndexCreationTask {
    public Creatives_ClickActions_1() {
      map = "docs.DroneStateSnapshots.SelectMany( x=> x.ClickActions, (snapshoot, x) => new { ClickedBy = new[] { x.ContactId}, x.CreativeId})";
      reduce = "results.GroupBy(x => x.CreativeId).Select(x => new { ClickedBy = x.SelectMany( m => m.ClickedBy).ToArray(), CreativeId = x.Key})";
    }
  }

  public static class Creatives_ClickActions_2 extends AbstractIndexCreationTask {
    public Creatives_ClickActions_2() {
      map = "docs.DroneStateSnapshots.SelectMany( x => x.ClickActions).Select(x=> new { ClickedBy = new[] { x.ContactId}, x.CreativeId})";
      reduce = "results.GroupBy(x=> x.CreativeId).Select(x=> new { ClickedBy = x.SelectMany(m => m.ClickedBy).ToArray(), CreativeId = x.Key})";
    }
  }
}
