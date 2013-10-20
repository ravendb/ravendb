package net.ravendb.tests.queries;

import java.util.UUID;

import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.document.DocumentStore;
import net.ravendb.tests.queries.QCanQueryOnLargeXmlTest_Item;

import org.junit.Test;


import com.mysema.query.annotations.QueryEntity;

public class CanQueryOnLargeXmlTest extends RemoteClientTest {
  @QueryEntity
  public static class Item {
    private String schemaFullName;
    private String valueBlobString;

    public String getSchemaFullName() {
      return schemaFullName;
    }
    public void setSchemaFullName(String schemaFullName) {
      this.schemaFullName = schemaFullName;
    }
    public String getValueBlobString() {
      return valueBlobString;
    }
    public void setValueBlobString(String valueBlobString) {
      this.valueBlobString = valueBlobString;
    }
  }

  @Test
  public void remote() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {

        String xml ="<?xml version=\"1.0\" encoding=\"utf-8\"?>\r\n<__Root__>\r\n <_STD s=\"DateTime\" t=\"System.DateTime, mscorlib, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089\" /> <dateTime xmlns=\"http://schemas.microsoft.com/2003/10/Serialization/\">2009-01-14T23:00:57.087Z</dateTime> </__Root__>";
        String guid = UUID.randomUUID().toString();

        QCanQueryOnLargeXmlTest_Item x = QCanQueryOnLargeXmlTest_Item.item;
        session.query(Item.class).where(x.schemaFullName.eq(guid).and(x.valueBlobString.eq(xml)));
        session.query(Item.class).where(x.schemaFullName.eq(guid).and(x.valueBlobString.eq(xml))).toList();
      }
    }
  }
}
