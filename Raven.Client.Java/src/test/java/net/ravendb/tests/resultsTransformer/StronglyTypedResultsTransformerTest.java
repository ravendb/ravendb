package net.ravendb.tests.resultsTransformer;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.document.DocumentStore;
import net.ravendb.client.indexes.AbstractTransformerCreationTask;
import net.ravendb.tests.resultsTransformer.QStronglyTypedResultsTransformerTest_Order;
import net.ravendb.tests.resultsTransformer.StronglyTypedResultsTransformerTest.OrderWithProductInformation.Result;
import net.ravendb.tests.resultsTransformer.StronglyTypedResultsTransformerTest.OrderWithProductInformation.ResultProduct;

import org.junit.Test;


import com.mysema.query.annotations.QueryEntity;

public class StronglyTypedResultsTransformerTest extends RemoteClientTest {
  public static class Product {
    private String id;
    private String name;
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
    public Product(String id, String name) {
      super();
      this.id = id;
      this.name = name;
    }
    public Product() {
      super();
    }

  }

  @QueryEntity
  public static class Order {


    public Order() {
      super();
    }
    public Order(String id, String customerId, List<String> productIds) {
      super();
      this.id = id;
      this.customerId = customerId;
      this.productIds = productIds;
    }
    private String id;
    private String customerId;
    private List<String> productIds;
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

  public static class OrderWithProductInformationMultipleReturns extends AbstractTransformerCreationTask {
    public static class Result {
      private String orderId;
      private String productId;
      private String productName;
      public String getOrderId() {
        return orderId;
      }
      public void setOrderId(String orderId) {
        this.orderId = orderId;
      }
      public String getProductId() {
        return productId;
      }
      public void setProductId(String productId) {
        this.productId = productId;
      }
      public String getProductName() {
        return productName;
      }
      public void setProductName(String productName) {
        this.productName = productName;
      }

    }

    public OrderWithProductInformationMultipleReturns() {

      transformResults = " from doc in results " +
                              " from productid in doc.ProductIds.AsEnumerable() " +
                              " let product = LoadDocument(productid) " +
                              " select new " +
                              " { " +
                              "     OrderId = doc.Id, " +
                              "     ProductId = product.Id, " +
                              "     ProductName = product.Name " +
                              " };";
    }

    @Override
    public String getTransformerName() {
      return "orderWithProdInfo";
    }
  }

  public static class OrderWithProductInformation extends AbstractTransformerCreationTask  {
    public static class Result {
      private String orderId;
      private String customerId;
      private ResultProduct[] products;
      public String getOrderId() {
        return orderId;
      }
      public void setOrderId(String orderId) {
        this.orderId = orderId;
      }
      public String getCustomerId() {
        return customerId;
      }
      public void setCustomerId(String customerId) {
        this.customerId = customerId;
      }
      public ResultProduct[] getProducts() {
        return products;
      }
      public void setProducts(ResultProduct[] products) {
        this.products = products;
      }

    }

    public static class ResultProduct {
      private String productId;
      private String productName;
      public String getProductId() {
        return productId;
      }
      public void setProductId(String productId) {
        this.productId = productId;
      }
      public String getProductName() {
        return productName;
      }
      public void setProductName(String productName) {
        this.productName = productName;
      }

    }

    public OrderWithProductInformation() {
      transformResults = "from doc in results" +
      		" select new {" +
      		"     OrderId = doc.Id," +
      		"     Products = from productid in (IEnumerable<object>)doc.ProductIds" +
      		"                let product = LoadDocument(productid)" +
      		"                select new                {" +
      		"                     ProductId = product.Id," +
      		"                     ProductName = product.Name" +
      		"                } };";

    }
  }


  @Test
  public void canUseResultsTransformerOnLoadWithRemoteDatabase() throws Exception {

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      new OrderWithProductInformation().execute(store);

      try (IDocumentSession session = store.openSession()) {
        session.store(new Product("products/milk", "Milk"));
        session.store(new Product("products/bear", "Bear"));

        Order order = new Order();
        order.setId("orders/1");
        order.setCustomerId("customers/ayende");
        order.setProductIds(Arrays.asList("products/milk", "products/bear"));

        session.store(order);
        session.saveChanges();
      }

      try (IDocumentSession session = store.openSession()) {
        Result order = session.load(OrderWithProductInformation.class, OrderWithProductInformation.Result.class, "orders/1");
        Arrays.sort(order.getProducts(), new Comparator<ResultProduct>() {
          @Override
          public int compare(ResultProduct o1, ResultProduct o2) {
            return o2.getProductName().compareTo(o1.getProductName());
          }
        });
        assertEquals("Milk", order.getProducts()[0].getProductName());
        assertEquals("products/milk", order.getProducts()[0].getProductId());
      }
    }
  }

  @Test
  public void canUseResultsTransformerOnLoadWithMultipleReturnsWithRemoteDatabase() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      new OrderWithProductInformationMultipleReturns().execute(store);

      try (IDocumentSession session = store.openSession()) {
        session.store(new Product("products/milk", "Milk"));
        session.store(new Product("products/bear", "Bear"));

        Order order = new Order();
        order.setId("orders/1");
        order.setCustomerId("customers/ayende");
        order.setProductIds(Arrays.asList("products/milk", "products/bear"));

        session.store(order);
        session.saveChanges();
      }

      try (IDocumentSession session = store.openSession()) {
        OrderWithProductInformationMultipleReturns.Result[] products = session.load(OrderWithProductInformationMultipleReturns.class, OrderWithProductInformationMultipleReturns.Result[].class, "orders/1");
        Arrays.sort(products, new Comparator<OrderWithProductInformationMultipleReturns.Result>() {
          @Override
          public int compare(OrderWithProductInformationMultipleReturns.Result o1, OrderWithProductInformationMultipleReturns.Result o2) {
            return o1.getProductId().compareTo(o2.getProductId());
          }
        });
        assertEquals("products/bear", products[0].getProductId());
        assertEquals("products/milk", products[1].getProductId());
      }
    }
  }


  /**
   * cannotUseResultsTransformerOnLoadWithMultipleReturnsSingleExpectation
   * @throws Exception
   */
  @Test(expected = IllegalStateException.class)
  public void cannotUseResults1() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      new OrderWithProductInformationMultipleReturns().execute(store);

      try (IDocumentSession session = store.openSession()) {
        session.store(new Product("products/milk", "Milk"));
        session.store(new Product("products/bear", "Bear"));

        Order order = new Order();
        order.setId("orders/1");
        order.setCustomerId("customers/ayende");
        order.setProductIds(Arrays.asList("products/milk", "products/bear"));

        session.store(order);
        session.saveChanges();
      }

      try (IDocumentSession session = store.openSession()) {
       session.load(OrderWithProductInformationMultipleReturns.class, OrderWithProductInformationMultipleReturns.Result.class, "orders/1");
      }

    }
  }

  @Test
  public void canUseResultsTransformerOnDynamicQueryWithRemoteDatabase() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      new OrderWithProductInformation().execute(store);

      try (IDocumentSession session = store.openSession()) {
        session.store(new Product("products/milk", "Milk"));
        session.store(new Product("products/bear", "Bear"));

        session.store(new Order(null, "customers/ayende", Arrays.asList("products/milk")));
        session.store(new Order(null, "customers/ayende", Arrays.asList("products/milk")));
        session.store(new Order(null, "customers/ayende", Arrays.asList("products/bear", "products/milk")));

        session.store(new Order(null, "customers/rahien", Arrays.asList("products/bear")));
        session.store(new Order(null, "customers/bob", Arrays.asList("products/bear", "products/milk")));

        session.saveChanges();
      }

      QStronglyTypedResultsTransformerTest_Order o = new QStronglyTypedResultsTransformerTest_Order("o");
      try (IDocumentSession session  = store.openSession()) {

        OrderWithProductInformation.Result customer = session.query(Order.class).where(o.customerId.eq("customers/bob"))
        .transformWith(OrderWithProductInformation.class, OrderWithProductInformation.Result.class)
        .single();

        Arrays.sort(customer.getProducts(), new Comparator<ResultProduct>() {
          @Override
          public int compare(ResultProduct o1, ResultProduct o2) {
            return o1.getProductName().compareTo(o2.getProductName());
          }
        });

        assertEquals("Milk", customer.getProducts()[1].getProductName());
        assertEquals("products/milk", customer.getProducts()[1].getProductId());

        assertEquals("Bear", customer.getProducts()[0].getProductName());
        assertEquals("products/bear", customer.getProducts()[0].getProductId());

      }
    }

  }

}
