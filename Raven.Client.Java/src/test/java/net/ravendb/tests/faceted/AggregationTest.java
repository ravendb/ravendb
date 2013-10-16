package net.ravendb.tests.faceted;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.HashMap;
import java.util.Map;

import net.ravendb.abstractions.data.FacetResult;
import net.ravendb.abstractions.data.FacetResults;
import net.ravendb.abstractions.data.FacetValue;
import net.ravendb.abstractions.indexing.SortOptions;
import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.document.DocumentQueryCustomizationFactory;
import net.ravendb.client.document.DocumentStore;
import net.ravendb.client.indexes.AbstractIndexCreationTask;
import net.ravendb.tests.faceted.QAggregationTest_Order;

import org.junit.Test;


import com.mysema.query.annotations.QueryEntity;

public class AggregationTest extends RemoteClientTest {

  @QueryEntity
  public static class Order {
    private String product;
    private double total;
    private Currency currency;
    private int quantity;
    public String getProduct() {
      return product;
    }
    public void setProduct(String product) {
      this.product = product;
    }
    public double getTotal() {
      return total;
    }
    public void setTotal(double total) {
      this.total = total;
    }
    public Currency getCurrency() {
      return currency;
    }
    public void setCurrency(Currency currency) {
      this.currency = currency;
    }
    public int getQuantity() {
      return quantity;
    }
    public void setQuantity(int quantity) {
      this.quantity = quantity;
    }

  }

  public static enum Currency  {
    USD,
    EUR,
    NIS;
  }

  public static class Orders_All extends AbstractIndexCreationTask {
    public Orders_All() {
      map = "from order in docs.orders select new { order.Currency, order.Product, order.Total, order.Quantity }";
      QAggregationTest_Order o = QAggregationTest_Order.order;
      sort(o.total, SortOptions.DOUBLE);
      sort(o.quantity, SortOptions.INT);
    }

  }

  @Test
  public void canCorrectlyAggregate() throws Exception {

    QAggregationTest_Order o = QAggregationTest_Order.order;

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      new Orders_All().execute(store);
      try (IDocumentSession session = store.openSession()) {

        Order order1 = new Order();
        order1.setCurrency(Currency.EUR);
        order1.setProduct("Milk");
        order1.setTotal(3);
        session.store(order1);

        Order order2 = new Order();
        order2.setCurrency(Currency.NIS);
        order2.setProduct("Milk");
        order2.setTotal(9);
        session.store(order2);

        Order order3 = new Order();
        order3.setCurrency(Currency.EUR);
        order3.setProduct("iPhone");
        order3.setTotal(3333);
        session.store(order3);
        session.saveChanges();
      }
      waitForNonStaleIndexes(serverClient);

      try (IDocumentSession session = store.openSession()) {
        FacetResults r = session.query(Order.class, Orders_All.class)
            .customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults())
            .aggregateBy(o.product)
            .sumOn(o.total)
            .toList();

        FacetResult facetResult = r.getResults().get("Product");
        assertEquals(2, facetResult.getValues().size());

        Map<String, Double> sumLookup = new HashMap<>();
        for (FacetValue facetValue : facetResult.getValues()) {
          sumLookup.put(facetValue.getRange(), facetValue.getSum());
        }
        assertEquals(Double.valueOf(12.0), sumLookup.get("milk"), 0.001);
        assertEquals(Double.valueOf(3333), sumLookup.get("iphone"), 0.001);

      }
    }
  }

  @Test
  public void canCorrectlyAggregate_MultipleItems() throws Exception {
    QAggregationTest_Order o = QAggregationTest_Order.order;

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      new Orders_All().execute(store);
      try (IDocumentSession session = store.openSession()) {
        Order order1 = new Order();
        order1.setCurrency(Currency.EUR);
        order1.setProduct("Milk");
        order1.setTotal(3);
        session.store(order1);

        Order order2 = new Order();
        order2.setCurrency(Currency.NIS);
        order2.setProduct("Milk");
        order2.setTotal(9);
        session.store(order2);

        Order order3 = new Order();
        order3.setCurrency(Currency.EUR);
        order3.setProduct("iPhone");
        order3.setTotal(3333);
        session.store(order3);
        session.saveChanges();
      }
      waitForNonStaleIndexes(serverClient);

      try (IDocumentSession session = store.openSession()) {
        FacetResults r = session.query(Order.class, "Orders/All")
            .customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults())
            .aggregateBy(o.product)
            .sumOn(o.total)
            .andAggregateOn(o.currency)
            .sumOn(o.total)
            .toList();

        FacetResult facetResult = r.getResults().get("Product");
        assertEquals(2, facetResult.getValues().size());

        Map<String, Double> sumLookup = new HashMap<>();
        for (FacetValue facetValue : facetResult.getValues()) {
          sumLookup.put(facetValue.getRange(), facetValue.getSum());
        }
        assertEquals(Double.valueOf(12.0), sumLookup.get("milk"), 0.001);
        assertEquals(Double.valueOf(3333), sumLookup.get("iphone"), 0.001);

        facetResult = r.getResults().get("Currency");
        assertEquals(2, facetResult.getValues().size());

        sumLookup = new HashMap<>();
        for (FacetValue facetValue : facetResult.getValues()) {
          sumLookup.put(facetValue.getRange(), facetValue.getSum());
        }
        assertEquals(Double.valueOf(3336), sumLookup.get(Currency.EUR.name().toLowerCase()), 0.001);
        assertEquals(Double.valueOf(9), sumLookup.get(Currency.NIS.name().toLowerCase()), 0.001);

      }
    }
  }

  @Test
  public void canCorrectlyAggregate_MultipleAggregations() throws Exception {
    QAggregationTest_Order o = QAggregationTest_Order.order;

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      new Orders_All().execute(store);
      try (IDocumentSession session = store.openSession()) {
        Order order1 = new Order();
        order1.setCurrency(Currency.EUR);
        order1.setProduct("Milk");
        order1.setTotal(3);
        session.store(order1);

        Order order2 = new Order();
        order2.setCurrency(Currency.NIS);
        order2.setProduct("Milk");
        order2.setTotal(9);
        session.store(order2);

        Order order3 = new Order();
        order3.setCurrency(Currency.EUR);
        order3.setProduct("iPhone");
        order3.setTotal(3333);
        session.store(order3);
        session.saveChanges();
      }
      waitForNonStaleIndexes(serverClient);

      try (IDocumentSession session = store.openSession()) {
        FacetResults r = session.query(Order.class, "Orders/All")
            .customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults())
            .aggregateBy(o.product)
            .maxOn(o.total)
            .minOn(o.total)
            .toList();

        FacetResult facetResult = r.getResults().get("Product");
        assertEquals(2, facetResult.getValues().size());

        Map<String, Double> minLookup = new HashMap<>();
        Map<String, Double> maxLookup = new HashMap<>();
        for (FacetValue facetValue : facetResult.getValues()) {
          minLookup.put(facetValue.getRange(), facetValue.getMin());
          maxLookup.put(facetValue.getRange(), facetValue.getMax());
        }
        assertEquals(Double.valueOf(3), minLookup.get("milk"), 0.001);
        assertEquals(Double.valueOf(9), maxLookup.get("milk"), 0.001);

        assertEquals(Double.valueOf(3333), minLookup.get("iphone"), 0.001);
        assertEquals(Double.valueOf(3333), maxLookup.get("iphone"), 0.001);

      }
    }
  }

  @Test
  public void canCorrectlyAggregate_DisplayName() throws Exception {
    QAggregationTest_Order o = QAggregationTest_Order.order;

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      new Orders_All().execute(store);
      try (IDocumentSession session = store.openSession()) {
        Order order1 = new Order();
        order1.setCurrency(Currency.EUR);
        order1.setProduct("Milk");
        order1.setTotal(3);
        session.store(order1);

        Order order2 = new Order();
        order2.setCurrency(Currency.NIS);
        order2.setProduct("Milk");
        order2.setTotal(9);
        session.store(order2);

        Order order3 = new Order();
        order3.setCurrency(Currency.EUR);
        order3.setProduct("iPhone");
        order3.setTotal(3333);
        session.store(order3);
        session.saveChanges();
      }
      waitForNonStaleIndexes(serverClient);

      try (IDocumentSession session = store.openSession()) {
        FacetResults r = session.query(Order.class, "Orders/All")
            .aggregateBy(o.product, "ProductMax")
            .maxOn(o.total)
            .andAggregateOn(o.product, "ProductMin")
            .countOn(o.currency)
            .toList();

        assertEquals(2, r.getResults().size());

        assertNotNull(r.getResults().get("ProductMax"));
        assertNotNull(r.getResults().get("ProductMin"));

        assertEquals(Double.valueOf(3333), r.getResults().get("ProductMax").getValues().get(0).getMax());
        assertEquals(Double.valueOf(2), r.getResults().get("ProductMin").getValues().get(1).getCount());


      }
    }
  }

  @Test
  public void canCorrectlyAggregate_Ranges() throws Exception {
    QAggregationTest_Order o = QAggregationTest_Order.order;

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      new Orders_All().execute(store);
      try (IDocumentSession session = store.openSession()) {
        Order order1 = new Order();
        order1.setCurrency(Currency.EUR);
        order1.setProduct("Milk");
        order1.setTotal(3);
        session.store(order1);

        Order order2 = new Order();
        order2.setCurrency(Currency.NIS);
        order2.setProduct("Milk");
        order2.setTotal(9);
        session.store(order2);

        Order order3 = new Order();
        order3.setCurrency(Currency.EUR);
        order3.setProduct("iPhone");
        order3.setTotal(3333);
        session.store(order3);
        session.saveChanges();
      }
      waitForNonStaleIndexes(serverClient);

      try (IDocumentSession session = store.openSession()) {
        FacetResults r = session.query(Order.class, "Orders/All")
            .aggregateBy(o.product)
            .sumOn(o.total)
            .andAggregateOn(o.total)
            .addRanges(o.total.lt(100),
                o.total.goe(100).and(o.total.lt(500)),
                o.total.goe(500).and(o.total.lt(1500)),
                o.total.goe(1500))
                .sumOn(o.total)
                .toList();

        FacetResult facetResult = r.getResults().get("Product");
        assertEquals(2, facetResult.getValues().size());

        Map<String, Double> sumLookup = new HashMap<>();
        for (FacetValue facetValue : facetResult.getValues()) {
          sumLookup.put(facetValue.getRange(), facetValue.getSum());
        }
        assertEquals(Double.valueOf(12.0), sumLookup.get("milk"), 0.001);
        assertEquals(Double.valueOf(3333), sumLookup.get("iphone"), 0.001);

        facetResult = r.getResults().get("Total");
        assertEquals(4, facetResult.getValues().size());

        sumLookup = new HashMap<>();
        for (FacetValue facetValue : facetResult.getValues()) {
          sumLookup.put(facetValue.getRange(), facetValue.getSum());
        }
        assertEquals(Double.valueOf(12.0), sumLookup.get("[NULL TO Dx100]"), 0.001);
        assertEquals(Double.valueOf(3333), sumLookup.get("{Dx1500 TO NULL]"), 0.001);


      }
    }
  }
}
