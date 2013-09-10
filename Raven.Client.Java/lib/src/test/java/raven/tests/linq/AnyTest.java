package raven.tests.linq;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.mysema.query.annotations.QueryEntity;

import raven.client.IDocumentSession;
import raven.client.IDocumentStore;
import raven.client.RemoteClientTest;
import raven.client.document.DocumentStore;

public class AnyTest extends RemoteClientTest {

  @QueryEntity
  public static class TestDoc  {
    private String someProperty;
    private List<String> stringArray;

    public String getSomeProperty() {
      return someProperty;
    }
    public void setSomeProperty(String someProperty) {
      this.someProperty = someProperty;
    }
    public List<String> getStringArray() {
      return stringArray;
    }
    public void setStringArray(List<String> stringArray) {
      this.stringArray = stringArray;
    }
  }

  @Test
  public void canQueryArrayWithAny() throws Exception {

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        TestDoc doc1 = new TestDoc();
        doc1.setStringArray(Arrays.asList("test2", "doc2", "foo2"));
        session.store(doc1);

        TestDoc doc2 = new TestDoc();
        doc2.setStringArray(Arrays.asList("doc", "foo"));
        session.store(doc2);
        session.saveChanges();
      }

      try (IDocumentSession session = store.openSession()) {
        TestDoc otherDoc = new TestDoc();
        otherDoc.setSomeProperty("foo");

        QAnyTest_TestDoc x = QAnyTest_TestDoc.testDoc;

        TestDoc doc = session.query(TestDoc.class).where(x.stringArray.any().eq(otherDoc.someProperty)).firstOrDefault();

        //TODO: finish me
        assertNotNull(doc);
        assertEquals(2, doc.stringArray.size());
      }
    }
  }
}
