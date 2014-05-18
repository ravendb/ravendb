package net.ravendb.tests.nestedindexing;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.document.DocumentQueryCustomizationFactory;
import net.ravendb.client.document.DocumentStore;
import net.ravendb.client.indexes.AbstractIndexCreationTask;
import net.ravendb.tests.nestedindexing.QWithMapReduceTest_ProductSalesByZip_Result;
import net.ravendb.tests.nestedindexing.WithMapReduceTest.ProductSalesByZip.Result;

import org.junit.Test;

import com.mysema.query.annotations.QueryEntity;


public class WithMapReduceTest extends RemoteClientTest {
  public static class Product {
    private String id;
    private String name;

    public Product() {
      super();
    }
    public Product(String id, String name) {
      super();
      this.id = id;
      this.name = name;
    }
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
  }

  public static class Order {
    private String id;
    private String customerId;
    private List<String> productIds;


    public Order() {
      super();
    }
    public Order(String id, String customerId, List<String> productIds) {
      super();
      this.id = id;
      this.customerId = customerId;
      this.productIds = productIds;
    }
    public String getId() {
      return id;
    }
    public void setId(String id) {
      this.id = id;
    }
    public String getCustomerId() {
      return customerId;
    }
    public void setCustomerId(String customerId) {
      this.customerId = customerId;
    }
    public List<String> getProductIds() {
      return productIds;
    }
    public void setProductIds(List<String> productIds) {
      this.productIds = productIds;
    }
  }

  public static class Customer {

    private String id;
    private String name;
    private String zipCode;


    public Customer() {
      super();
    }
    public Customer(String id, String name, String zipCode) {
      super();
      this.id = id;
      this.name = name;
      this.zipCode = zipCode;
    }
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
    public String getZipCode() {
      return zipCode;
    }
    public void setZipCode(String zipCode) {
      this.zipCode = zipCode;
    }

  }

  public static class ProductSalesByZip extends AbstractIndexCreationTask {
    @QueryEntity
    public static class Result {
      private String zip;
      private String productId;
      private int count;
      public String getZip() {
        return zip;
      }
      public void setZip(String zip) {
        this.zip = zip;
      }
      public String getProductId() {
        return productId;
      }
      public void setProductId(String productId) {
        this.productId = productId;
      }
      public int getCount() {
        return count;
      }
      public void setCount(int count) {
        this.count = count;
      }
    }
    public ProductSalesByZip() {
      map = "from order in docs.Orders" +
          " let zip = LoadDocument(order.CustomerId).ZipCode" +
          " from p in order.ProductIds " +
          "select new { Zip = zip, ProductId = p, Count = 1} ";
      reduce = "from result in results group result by new { result.Zip, result.ProductId} into g select new { g.Key.Zip, g.Key.ProductId, Count = g.Sum(x => x.Count) }";
    }
  }

  @Test
  public void canUseReferencesFromMapReduceMap() throws Exception {

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      new ProductSalesByZip().execute(store);
      try (IDocumentSession session = store.openSession()) {
        session.store(new Product("Milk", "products/milk"));
        session.store(new Product("Bear", "products/bear"));

        session.store(new Customer("customers/ayende", "Ayende", "1234"));
        session.store(new Customer("customers/rahien", "Rahien", "4321"));

        session.store(new Order(null, "customers/ayende", Arrays.asList("products/milk")));
        session.store(new Order(null, "customers/ayende", Arrays.asList("products/milk")));
        session.store(new Order(null, "customers/ayende", Arrays.asList("products/bear", "products/milk")));

        session.store(new Order(null, "customers/rahien", Arrays.asList("products/bear")));
        session.store(new Order(null, "customers/rahien", Arrays.asList("products/bear", "products/milk")));

        session.saveChanges();
      }

      QWithMapReduceTest_ProductSalesByZip_Result x = QWithMapReduceTest_ProductSalesByZip_Result.result;
      try (IDocumentSession session = store.openSession()) {
        List<Result> results = session.query(ProductSalesByZip.Result.class, ProductSalesByZip.class)
          .customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults())
          .orderBy(x.zip.asc(), x.productId.asc())
          .toList();

        assertEquals(4, results.size());

        assertEquals("1234", results.get(0).getZip());
        assertEquals("products/bear", results.get(0).getProductId());
        assertEquals(1, results.get(0).getCount());

        assertEquals("1234", results.get(1).getZip());
        assertEquals("products/milk", results.get(1).getProductId());
        assertEquals(3, results.get(1).getCount());

        assertEquals("4321", results.get(2).getZip());
        assertEquals("products/bear", results.get(2).getProductId());
        assertEquals(2, results.get(2).getCount());

        assertEquals("4321", results.get(3).getZip());
        assertEquals("products/milk", results.get(3).getProductId());
        assertEquals(1, results.get(3).getCount());


      }
    }
  }
}
