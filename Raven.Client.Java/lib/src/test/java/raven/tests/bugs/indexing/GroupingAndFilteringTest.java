package raven.tests.bugs.indexing;

import java.util.EnumSet;

import org.junit.Test;

import raven.abstractions.data.AggregationOperation;
import raven.abstractions.json.linq.RavenJObject;
import raven.client.IDocumentSession;
import raven.client.IDocumentStore;
import raven.client.RemoteClientTest;
import raven.client.document.DocumentStore;

public class GroupingAndFilteringTest extends RemoteClientTest {

  @Test
  public void onSameField() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {

        session.advanced().luceneQuery(RavenJObject.class)
        .whereEquals("Tags,Name", "Ayende")
        .groupBy(EnumSet.of(AggregationOperation.COUNT), "Tags,Name").toList();

      }
    }
  }

  @Test
  public void onSameFieldDynamically() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {

        session.advanced().luceneQuery(RavenJObject.class)
        .whereEquals("Tags,Name", "Ayende")
        .groupBy(EnumSet.of(AggregationOperation.COUNT, AggregationOperation.DISTINCT), "Tags,Name").toList();

      }
    }
  }
}
