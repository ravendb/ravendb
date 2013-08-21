package raven.tests.resultsTransformer;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;


import org.junit.Test;

import com.mysema.query.annotations.QueryEntity;
import com.mysema.query.types.expr.BooleanExpression;

import raven.client.IDocumentSession;
import raven.client.IDocumentStore;
import raven.client.RemoteClientTest;
import raven.client.document.DocumentStore;
import raven.client.indexes.AbstractTransformerCreationTask;
import raven.linq.dsl.TransformerExpression;
import raven.linq.dsl.expressions.AnonymousExpression;
import raven.tests.resultsTransformer.StronglyTypedResultsTransformerTest.OrderWithProductInformation.Result;
import raven.tests.resultsTransformer.StronglyTypedResultsTransformerTest.OrderWithProductInformation.ResultProduct;

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
      transformResultsExpression = TransformerExpression.from("results");
      //TODO: finish
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
      //TODO:
      transformResults = "from doc in results " +
                                          " select new                                                        " +
                                          " {                                                                 " +
                                          "   OrderId = doc.Id,                                               " +
                                          "       Products = from productid in (IEnumerable<string>)doc.ProductIds    " +
                                          "                  let product = LoadDocument(productid)   " +
                                          "                  select new                                       " +
                                          "                  {                                                " +
                                          "                       ProductId = product.Id,                     " +
                                          "                       ProductName = product.Name                  " +
                                          "                  }                                                " +
                                          " }";

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
        order.setProductIds(Arrays.asList("products/milk"));

        session.store(order);
        session.saveChanges();
      }

      try (IDocumentSession session = store.openSession()) {
        Result order = session.load(OrderWithProductInformation.class, OrderWithProductInformation.Result.class, "orders/1");
        Arrays.sort(order.getProducts(), new Comparator<ResultProduct>() {
          @Override
          public int compare(ResultProduct o1, ResultProduct o2) {
            return o1.getProductName().compareTo(o2.getProductName());
          }
        });
        assertEquals("Milk", order.getProducts()[0].getProductName());
        assertEquals("products/milk", order.getProducts()[0].getProductId());
      }
    }
  }
  //TODO: other tests




}
